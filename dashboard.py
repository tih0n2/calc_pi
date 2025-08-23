#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
📊 Дашборд аналитики калькулятора инвестиций
Подключение к PostgreSQL и визуализация статистики
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import openpyxl
import ssl
import os
from datetime import datetime, timedelta
import numpy as np
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройки страницы
st.set_page_config(
    page_title="Аналитика Калькулятора Инвестиций", 
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Стили
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #667eea;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .metric-label {
        font-size: 1rem;
        opacity: 0.9;
    }
    
    /* Фиксация размеров графиков */
    .stPlotlyChart {
        height: auto !important;
        max-height: 500px !important;
        overflow: hidden !important;
    }
    
    /* Стабилизация контейнеров */
    div[data-testid="stVerticalBlock"] > div.element-container {
        height: auto !important;
    }
    
    /* Фиксация для мобильных устройств */
    @media (max-width: 768px) {
        .stPlotlyChart {
            max-height: 400px !important;
        }
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=3600)  # Кэш курсов на 1 час
def get_cbr_rates():
    """Получение курсов валют от ЦБ РФ"""
    try:
        url = "http://www.cbr.ru/scripts/XML_daily.asp"
        response = requests.get(url, timeout=10)
        response.encoding = 'windows-1251'
        
        root = ET.fromstring(response.content)
        
        rates = {'RUB': 1.0}  # Рубль = 1
        
        for valute in root.findall('Valute'):
            char_code = valute.find('CharCode').text
            value = valute.find('Value').text.replace(',', '.')
            nominal = valute.find('Nominal').text
            
            if char_code in ['USD', 'EUR']:
                rate = float(value) / float(nominal)
                rates[char_code] = rate
        
        return rates
        
    except Exception as e:
        st.warning(f"⚠️ Не удалось получить курсы ЦБ РФ: {e}")
        # Возвращаем примерные курсы как fallback
        return {'RUB': 1.0, 'USD': 95.0, 'EUR': 105.0}

def convert_to_rub(amount, currency, rates):
    """Конвертация суммы в рубли"""
    if currency == 'RUB':
        return amount
    return amount * rates.get(currency, 1.0)

def get_database_connection():
    """Подключение к PostgreSQL с переподключением"""
    try:
        # Настройки подключения из переменных окружения
        connection_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'database': os.getenv('DB_NAME', 'calcus_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'sslmode': os.getenv('DB_SSLMODE', 'prefer'),
            'sslcert': os.getenv('DB_SSLCERT'),
            'sslkey': os.getenv('DB_SSLKEY'),
            'sslrootcert': os.getenv('DB_SSLROOTCERT'),
            'connect_timeout': 10,
            'application_name': 'calcus_dashboard'
        }
        
        # Убираем None значения
        connection_params = {k: v for k, v in connection_params.items() if v is not None}
        
        conn = psycopg2.connect(**connection_params)
        return conn
    
    except Exception as e:
        st.error(f"❌ Ошибка подключения к БД: {e}")
        st.stop()

def execute_query_with_retry(query, max_retries=3):
    """Выполнение запроса с переподключением при ошибке"""
    for attempt in range(max_retries):
        try:
            conn = get_database_connection()
            if conn is None:
                raise Exception("Не удалось подключиться к базе данных")
            
            # Проверяем, что соединение активно
            if conn.closed:
                conn = get_database_connection()
            
            df = pd.read_sql(query, conn)
            conn.close()
            return df
            
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            if attempt < max_retries - 1:
                st.warning(f"⚠️ Переподключение к БД (попытка {attempt + 1}/{max_retries})")
                continue
            else:
                st.error(f"❌ Ошибка БД после {max_retries} попыток: {e}")
                st.stop()
        except Exception as e:
            st.error(f"❌ Ошибка загрузки данных: {e}")
            st.stop()

@st.cache_data(ttl=60)  # Кэш на 1 минуту для быстрых данных
def load_data():
    """Загрузка данных из БД"""
    
    query = """
    SELECT 
        id,
        client_id,
        created_at,
        user_ip,
        user_agent,
        calculation_type,
        initial_sum,
        target_sum,
        period,
        period_unit,
        interest_rate,
        reinvest_enabled,
        reinvest_period,
        add_period,
        add_sum,
        currency,
        final_amount,
        total_profit,
        total_contributions,
        effective_rate,
        time_months,
        time_formatted,
        api_response_time_ms,
        calculation_version,
        DATE(created_at) as date_only,
        EXTRACT(hour FROM created_at) as hour_only,
        EXTRACT(dow FROM created_at) as day_of_week
    FROM investment_calculations 
    ORDER BY created_at DESC
    """
    
    # Используем функцию с переподключением
    df = execute_query_with_retry(query)
    
    # Обработка данных
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    return df

def format_currency(amount, currency='RUB'):
    """Форматирование валюты"""
    if currency == 'RUB':
        return f"{amount:,.0f} ₽"
    elif currency == 'USD':
        return f"${amount:,.0f}"
    elif currency == 'EUR':
        return f"€{amount:,.0f}"
    else:
        return f"{amount:,.0f} {currency}"

def main():
    # Заголовок
    st.markdown('<h1 class="main-header">Аналитика Калькулятора Инвестиций</h1>', unsafe_allow_html=True)
    
    # Загрузка данных
    with st.spinner("Загружаем данные..."):
        try:
            df = load_data()
            
            if df.empty:
                st.warning("📭 Данных пока нет. Сделайте несколько расчетов в калькуляторе!")
                st.stop()
                
        except Exception as e:
            st.error(f"❌ Ошибка загрузки данных: {e}")
            st.stop()
    
    # Боковая панель с фильтрами
    st.sidebar.header("Фильтры")
    
    # Переключатель валют
    convert_to_rubles = st.sidebar.checkbox(
        "Пересчитать все в рубли по курсу ЦБ РФ",
        value=False,
        help="При включении все суммы будут пересчитаны в рубли по текущему курсу ЦБ РФ"
    )
    
    # Фильтр по дате
    date_range = st.sidebar.date_input(
        "Период",
        value=(df['date_only'].min().date(), df['date_only'].max().date()),
        min_value=df['date_only'].min().date(),
        max_value=df['date_only'].max().date()
    )
    
    # Фильтр по валютам
    currencies = st.sidebar.multiselect(
        "Валюты",
        options=df['currency'].unique(),
        default=df['currency'].unique()
    )
    
    # Фильтр по типам расчетов
    calculation_types = {
        1: "Итоговая сумма",
        4: "Срок достижения цели"
    }
    
    # Получаем уникальные типы из данных
    available_types = df['calculation_type'].unique()
    type_options = [calculation_types.get(t, f"Тип {t}") for t in available_types]
    
    selected_calc_types = st.sidebar.multiselect(
        "Типы расчетов",
        options=type_options,
        default=type_options
    )
    
    # Преобразуем обратно в числовые значения
    selected_type_numbers = []
    for selected_type in selected_calc_types:
        for num, name in calculation_types.items():
            if name == selected_type:
                selected_type_numbers.append(num)
                break
        else:
            # Если не нашли в словаре, это "Тип X"
            if selected_type.startswith("Тип "):
                type_num = int(selected_type.split(" ")[1])
                selected_type_numbers.append(type_num)
    
    # Дополнительные фильтры по параметрам расчетов
    st.sidebar.markdown("---")
    st.sidebar.subheader("Фильтры по параметрам")
    
    # Фильтр по начальной сумме
    min_initial = float(df['initial_sum'].min())
    max_initial = float(df['initial_sum'].max())
    initial_sum_range = st.sidebar.slider(
        "Начальная сумма (₽)",
        min_value=min_initial,
        max_value=max_initial,
        value=(min_initial, max_initial),
        step=100000.0,
        format="%.0f"
    )
    
    # Фильтр по процентной ставке
    min_rate = float(df['interest_rate'].min())
    max_rate = float(df['interest_rate'].max())
    interest_rate_range = st.sidebar.slider(
        "Процентная ставка (%)",
        min_value=min_rate,
        max_value=max_rate,
        value=(min_rate, max_rate),
        step=0.5,
        format="%.1f"
    )
    
    # Фильтр по периоду (только для типа 1)
    if 1 in selected_type_numbers:
        df_type1 = df[df['calculation_type'] == 1]
        if not df_type1.empty:
            min_period = int(df_type1['period'].min())
            max_period = int(df_type1['period'].max())
            period_range = st.sidebar.slider(
                "Период инвестиций (мес.)",
                min_value=min_period,
                max_value=max_period,
                value=(min_period, max_period),
                step=1
            )
        else:
            period_range = None
    else:
        period_range = None
    
    # Фильтр по целевой сумме (только для типа 4)
    if 4 in selected_type_numbers:
        df_type4 = df[df['calculation_type'] == 4]
        if not df_type4.empty and df_type4['target_sum'].notna().any():
            min_target = float(df_type4['target_sum'].min())
            max_target = float(df_type4['target_sum'].max())
            target_sum_range = st.sidebar.slider(
                "Целевая сумма (₽)",
                min_value=min_target,
                max_value=max_target,
                value=(min_target, max_target),
                step=100000.0,
                format="%.0f"
            )
        else:
            target_sum_range = None
    else:
        target_sum_range = None
    
    # Фильтр по реинвестированию
    reinvest_options = st.sidebar.multiselect(
        "Реинвестирование",
        options=["С реинвестированием", "Без реинвестирования"],
        default=["С реинвестированием", "Без реинвестирования"]
    )
    
    # Преобразуем в булевы значения
    reinvest_values = []
    if "С реинвестированием" in reinvest_options:
        reinvest_values.append(True)
    if "Без реинвестирования" in reinvest_options:
        reinvest_values.append(False)
    
    # Фильтр по итоговой сумме
    min_final = float(df['final_amount'].min())
    max_final = float(df['final_amount'].max())
    final_amount_range = st.sidebar.slider(
        "Итоговая сумма (₽)",
        min_value=min_final,
        max_value=max_final,
        value=(min_final, max_final),
        step=100000.0,
        format="%.0f"
    )
    
    # Фильтр по прибыли
    min_profit = float(df['total_profit'].min())
    max_profit = float(df['total_profit'].max())
    profit_range = st.sidebar.slider(
        "Заработанная прибыль (₽)",
        min_value=min_profit,
        max_value=max_profit,
        value=(min_profit, max_profit),
        step=50000.0,
        format="%.0f"
    )
    
    # Фильтрация данных
    conditions = [
        (df['currency'].isin(currencies)),
        (df['calculation_type'].isin(selected_type_numbers)),
        (df['initial_sum'] >= initial_sum_range[0]),
        (df['initial_sum'] <= initial_sum_range[1]),
        (df['interest_rate'] >= interest_rate_range[0]),
        (df['interest_rate'] <= interest_rate_range[1]),
        (df['final_amount'] >= final_amount_range[0]),
        (df['final_amount'] <= final_amount_range[1]),
        (df['total_profit'] >= profit_range[0]),
        (df['total_profit'] <= profit_range[1])
    ]
    
    # Добавляем условие по дате если выбран диапазон
    if len(date_range) == 2:
        conditions.extend([
            (df['date_only'].dt.date >= date_range[0]),
            (df['date_only'].dt.date <= date_range[1])
        ])
    
    # Добавляем условие по периоду для типа 1
    if period_range is not None:
        conditions.append(
            (df['calculation_type'] != 1) | 
            ((df['period'] >= period_range[0]) & (df['period'] <= period_range[1]))
        )
    
    # Добавляем условие по целевой сумме для типа 4
    if target_sum_range is not None:
        conditions.append(
            (df['calculation_type'] != 4) | 
            ((df['target_sum'] >= target_sum_range[0]) & (df['target_sum'] <= target_sum_range[1]))
        )
    
    # Добавляем условие по реинвестированию
    if reinvest_values:
        conditions.append(df['reinvest_enabled'].isin(reinvest_values))
    
    # Применяем все условия
    df_filtered = df
    for condition in conditions:
        df_filtered = df_filtered[condition]
    
    if df_filtered.empty:
        st.warning("📭 Нет данных для выбранных фильтров!")
        st.stop()
    
    # Информация о примененных фильтрах
    total_records = len(df)
    filtered_records = len(df_filtered)
    filter_ratio = (filtered_records / total_records) * 100
    
    # Информация о записях с компактными кнопками экспорта
    col1, col2 = st.columns([4, 1])
    
    with col1:
        st.info(f"📊 Показано {filtered_records:,} из {total_records:,} записей ({filter_ratio:.1f}%)")
    
    with col2:
        # Компактные кнопки экспорта в одной строке
        subcol1, subcol2 = st.columns(2)
        
        with subcol1:
            # Экспорт в CSV
            csv_data = df_filtered.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📄",
                data=csv_data,
                file_name=f"calcus_data_{filtered_records}_records.csv",
                mime="text/csv",
                help=f"Скачать CSV ({filtered_records:,} записей)",
                use_container_width=True
            )
        
        with subcol2:
            # Экспорт в Excel
            import io
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df_filtered.to_excel(writer, index=False, sheet_name='Данные расчетов')
            excel_data = excel_buffer.getvalue()
            
            st.download_button(
                label="📊",
                data=excel_data,
                file_name=f"calcus_data_{filtered_records}_records.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help=f"Скачать Excel ({filtered_records:,} записей)",
                use_container_width=True
            )
    
    # Краткая сводка по примененным фильтрам (если они отличаются от максимальных)
    filter_summary = []
    
    if initial_sum_range[0] > min_initial or initial_sum_range[1] < max_initial:
        filter_summary.append(f"💰 Начальная сумма: {initial_sum_range[0]/1000000:.1f}М - {initial_sum_range[1]/1000000:.1f}М₽")
    
    if interest_rate_range[0] > min_rate or interest_rate_range[1] < max_rate:
        filter_summary.append(f"📈 Ставка: {interest_rate_range[0]:.1f}% - {interest_rate_range[1]:.1f}%")
    
    if final_amount_range[0] > min_final or final_amount_range[1] < max_final:
        filter_summary.append(f"🎯 Итоговая сумма: {final_amount_range[0]/1000000:.1f}М - {final_amount_range[1]/1000000:.1f}М₽")
    
    if profit_range[0] > min_profit or profit_range[1] < max_profit:
        filter_summary.append(f"💸 Прибыль: {profit_range[0]/1000000:.1f}М - {profit_range[1]/1000000:.1f}М₽")
    
    if len(reinvest_options) == 1:
        filter_summary.append(f"🔄 {reinvest_options[0]}")
    
    if filter_summary:
        st.caption("🔍 Активные фильтры: " + " • ".join(filter_summary))
    
    # Конвертация в рубли если выбрано
    if convert_to_rubles:
        df_filtered = df_filtered.copy()
        df_filtered['initial_sum'] = df_filtered.apply(
            lambda row: convert_to_rub(row['initial_sum'], row['currency'], rates), axis=1
        )
        df_filtered['final_amount'] = df_filtered.apply(
            lambda row: convert_to_rub(row['final_amount'], row['currency'], rates), axis=1
        )
        df_filtered['total_profit'] = df_filtered.apply(
            lambda row: convert_to_rub(row['total_profit'], row['currency'], rates), axis=1
        )
        df_filtered['add_sum'] = df_filtered.apply(
            lambda row: convert_to_rub(row['add_sum'], row['currency'], rates) if pd.notna(row['add_sum']) else 0, axis=1
        )
        # Помечаем что данные в рублях
        display_currency = "₽ (пересчет по курсу ЦБ РФ)"
    else:
        display_currency = "смешанные валюты"
    
    # Основные метрики
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{len(df_filtered):,}</div>
            <div class="metric-label">Всего расчетов</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        unique_users = df_filtered['client_id'].nunique()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{unique_users:,}</div>
            <div class="metric-label">Уникальных пользователей</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        median_initial = df_filtered['initial_sum'].median()
        currency_suffix = "₽" if convert_to_rubles else ""
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{median_initial/1000000:.1f}М{currency_suffix}</div>
            <div class="metric-label">Медианная начальная сумма</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        median_rate = df_filtered['interest_rate'].median()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{median_rate:.1f}%</div>
            <div class="metric-label">Медианная ставка</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Дополнительные метрики по суммам
    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        median_final = df_filtered['final_amount'].median()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{median_final/1000000:.1f}М{currency_suffix}</div>
            <div class="metric-label">Медианная итоговая сумма</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        median_profit = df_filtered['total_profit'].median()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{median_profit/1000000:.1f}М{currency_suffix}</div>
            <div class="metric-label">Медианная прибыль</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        total_invested = df_filtered['initial_sum'].sum()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{total_invested/1000000000:.1f}млрд{currency_suffix}</div>
            <div class="metric-label">Общий объем инвестиций</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        total_profit = df_filtered['total_profit'].sum()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{total_profit/1000000000:.1f}млрд{currency_suffix}</div>
            <div class="metric-label">Общая прибыль</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Анализ типов расчетов
    st.subheader("Анализ типов расчетов")
    
    calc_type_stats = df_filtered.groupby('calculation_type').agg({
        'id': 'count',
        'client_id': 'nunique'
    }).reset_index()
    
    calc_type_stats['type_name'] = calc_type_stats['calculation_type'].map(
        lambda x: calculation_types.get(x, f"Тип {x}")
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        # График распределения по типам
        fig_types = px.pie(
            calc_type_stats,
            values='id',
            names='type_name',
            title="Распределение по типам расчетов"
        )
        fig_types.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_types, use_container_width=True, key="calc_types_pie")
    
    with col2:
        # Таблица статистики по типам
        st.write("**Статистика по типам расчетов:**")
        display_stats = calc_type_stats[['type_name', 'id', 'client_id']].copy()
        display_stats.columns = ['Тип расчета', 'Количество', 'Уникальных пользователей']
        st.dataframe(display_stats, use_container_width=True)
    
    # Специальный анализ для "Срок достижения цели"
    time_goal_data = df_filtered[df_filtered['calculation_type'] == 4]
    if not time_goal_data.empty:
        st.subheader("Анализ расчетов 'Срок достижения цели'")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_time = time_goal_data['time_months'].mean()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{avg_time:.1f}</div>
                <div class="metric-label">Средний срок (мес.)</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            median_time = time_goal_data['time_months'].median()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{median_time:.1f}</div>
                <div class="metric-label">Медианный срок (мес.)</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            avg_target = time_goal_data['target_sum'].mean()
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{avg_target/1000000:.1f}М₽</div>
                <div class="metric-label">Средняя цель</div>
            </div>
            """, unsafe_allow_html=True)
        
        # График распределения времени достижения цели
        col1, col2 = st.columns(2)
        
        with col1:
            # Создаем диапазоны для времени
            time_bins = [0, 12, 24, 36, 60, 120, float('inf')]
            time_labels = ['<1 года', '1-2 года', '2-3 года', '3-5 лет', '5-10 лет', '>10 лет']
            
            time_goal_data_copy = time_goal_data.copy()
            time_goal_data_copy['time_range'] = pd.cut(
                time_goal_data_copy['time_months'],
                bins=time_bins,
                labels=time_labels
            )
            time_dist = time_goal_data_copy['time_range'].value_counts().sort_index()
            
            fig_time = px.bar(
                x=time_dist.index,
                y=time_dist.values,
                title="Распределение сроков достижения цели",
                color_discrete_sequence=['#667eea']
            )
            fig_time.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=40, b=20),
                xaxis_title="Срок",
                yaxis_title="Количество расчетов"
            )
            fig_time.update_traces(texttemplate='%{y}', textposition='auto')
            st.plotly_chart(fig_time, use_container_width=True, key="time_distribution")
        
        with col2:
            # График целевых сумм для расчетов времени
            target_bins = [0, 1000000, 5000000, 10000000, 50000000, float('inf')]
            target_labels = ['<1M', '1M-5M', '5M-10M', '10M-50M', '>50M']
            
            time_goal_data_copy['target_range'] = pd.cut(
                time_goal_data_copy['target_sum'],
                bins=target_bins,
                labels=target_labels
            )
            target_dist = time_goal_data_copy['target_range'].value_counts().sort_index()
            
            fig_target = px.bar(
                x=target_dist.index,
                y=target_dist.values,
                title="Распределение целевых сумм",
                color_discrete_sequence=['#667eea']
            )
            fig_target.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=40, b=20),
                xaxis_title="Целевая сумма",
                yaxis_title="Количество расчетов"
            )
            fig_target.update_traces(texttemplate='%{y}', textposition='auto')
            st.plotly_chart(fig_target, use_container_width=True, key="target_distribution")
    
    # Графики в две колонки
    col1, col2 = st.columns(2)
    
    # График активности по времени
    with col1:
        st.subheader("Активность по дням")
        daily_stats = df_filtered.groupby('date_only').agg({
            'id': 'count',
            'client_id': 'nunique'
        }).reset_index()
        daily_stats.columns = ['Дата', 'Расчеты', 'Уникальные пользователи']
        
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(
            x=daily_stats['Дата'], 
            y=daily_stats['Расчеты'],
            mode='lines+markers',
            name='Расчеты',
            line=dict(color='#667eea', width=3),
            marker=dict(size=8)
        ))
        fig1.add_trace(go.Scatter(
            x=daily_stats['Дата'], 
            y=daily_stats['Уникальные пользователи'],
            mode='lines+markers',
            name='Уникальные пользователи',
            line=dict(color='#f093fb', width=3),
            marker=dict(size=8)
        ))
        fig1.update_layout(
            height=380,
            margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            autosize=False,
            showlegend=True
        )
        st.plotly_chart(fig1, use_container_width=True, key="daily_activity")
    
    # Распределение по валютам
    with col2:
        st.subheader("Популярность валют")
        currency_stats = df_filtered.groupby('currency').agg({
            'id': 'count',
            'initial_sum': 'mean'
        }).reset_index()
        
        fig2 = px.pie(
            currency_stats, 
            values='id', 
            names='currency',
            title="",
            color_discrete_map={'RUB': '#667eea', 'USD': '#f093fb', 'EUR': '#36d1dc'}
        )
        fig2.update_traces(
            textposition='inside', 
            textinfo='percent+label',
            textfont_size=14,
            marker=dict(line=dict(color='#FFFFFF', width=2))
        )
        fig2.update_layout(
            height=380, 
            margin=dict(l=20, r=20, t=40, b=20),
            autosize=False
        )
        st.plotly_chart(fig2, use_container_width=True, key="currency_pie")
    
    # Большие графики на всю ширину
    st.subheader("Детальная аналитика по суммам")
    
    # Распределение сумм инвестиций
    col1, col2 = st.columns(2)
    
    with col1:
        # Убираем дублирующийся заголовок - остается только в title графика
        
        # Создаем диапазоны с учетом конвертации (максимальная детализация для малых сумм)
        if convert_to_rubles:
            bins = [0, 50000, 100000, 200000, 500000, 1000000, 2000000, 5000000, 10000000, float('inf')]
            labels = ['<50К₽', '50К-100К₽', '100К-200К₽', '200К-500К₽', '500К-1М₽', '1М-2М₽', '2М-5М₽', '5М-10М₽', '>10М₽']
        else:
            bins = [0, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000, 5000000, float('inf')]
            labels = ['<5K', '5K-10K', '10K-20K', '20K-50K', '50K-100K', '100K-200K', '200K-500K', '500K-1M', '1M-5M', '>5M']
        
        df_filtered['initial_sum_range'] = pd.cut(
            df_filtered['initial_sum'], 
            bins=bins,
            labels=labels
        )
        sum_dist = df_filtered['initial_sum_range'].value_counts().sort_index()
        
        fig3 = px.bar(
            x=sum_dist.index,
            y=sum_dist.values,
            title="Распределение начальных сумм",
            color_discrete_sequence=['#667eea']
        )
        fig3.update_layout(
            height=350,
            showlegend=False,
            margin=dict(l=20, r=20, t=40, b=20),
            autosize=False
        )
        fig3.update_traces(texttemplate='%{y}', textposition='auto')
        st.plotly_chart(fig3, use_container_width=True, key="initial_sums")
    
    with col2:
        # Убираем дублирующийся заголовок - остается только в title графика
        
        # Диапазоны для итоговых сумм (обычно больше начальных, больше детализации)
        if convert_to_rubles:
            bins_final = [0, 200000, 1000000, 2000000, 5000000, 10000000, 20000000, 50000000, float('inf')]
            labels_final = ['<200К₽', '200К-1М₽', '1М-2М₽', '2М-5М₽', '5М-10М₽', '10М-20М₽', '20М-50М₽', '>50М₽']
        else:
            bins_final = [0, 50000, 200000, 500000, 1000000, 2000000, 5000000, 10000000, float('inf')]
            labels_final = ['<50K', '50K-200K', '200K-500K', '500K-1M', '1M-2M', '2M-5M', '5M-10M', '>10M']
        
        df_filtered['final_amount_range'] = pd.cut(
            df_filtered['final_amount'], 
            bins=bins_final,
            labels=labels_final
        )
        final_dist = df_filtered['final_amount_range'].value_counts().sort_index()
        
        fig4 = px.bar(
            x=final_dist.index,
            y=final_dist.values,
            title="Распределение итоговых сумм",
            color_discrete_sequence=['#667eea']
        )
        fig4.update_layout(
            height=350,
            showlegend=False,
            margin=dict(l=20, r=20, t=40, b=20),
            autosize=False
        )
        fig4.update_traces(texttemplate='%{y}', textposition='auto')
        st.plotly_chart(fig4, use_container_width=True, key="final_amounts")
    
    # Новые графики распределения
    col1, col2 = st.columns(2)
    
    with col1:
        # Убираем дублирующийся заголовок - остается только в title графика
        
        # Диапазоны для прибыли (больше детализации)
        if convert_to_rubles:
            bins_profit = [0, 50000, 200000, 500000, 1000000, 2000000, 5000000, 10000000, float('inf')]
            labels_profit = ['<50К₽', '50К-200К₽', '200К-500К₽', '500К-1М₽', '1М-2М₽', '2М-5М₽', '5М-10М₽', '>10М₽']
        else:
            bins_profit = [0, 10000, 50000, 100000, 200000, 500000, 1000000, 2000000, float('inf')]
            labels_profit = ['<10K', '10K-50K', '50K-100K', '100K-200K', '200K-500K', '500K-1M', '1M-2M', '>2M']
        
        df_filtered['profit_range'] = pd.cut(
            df_filtered['total_profit'], 
            bins=bins_profit,
            labels=labels_profit
        )
        profit_dist = df_filtered['profit_range'].value_counts().sort_index()
        
        fig5 = px.bar(
            x=profit_dist.index,
            y=profit_dist.values,
            title="Распределение прибыли",
            color_discrete_sequence=['#667eea']
        )
        fig5.update_layout(
            height=350,
            showlegend=False,
            margin=dict(l=20, r=20, t=40, b=20),
            autosize=False
        )
        fig5.update_traces(texttemplate='%{y}', textposition='auto')
        st.plotly_chart(fig5, use_container_width=True, key="profit_dist")
    
    with col2:
        # Убираем дублирующийся заголовок - остается только в title графика
        fig6 = px.histogram(
            df_filtered, 
            x='interest_rate',
            nbins=15,
            color_discrete_sequence=['#667eea'],
            title="Частота использования процентных ставок"
        )
        fig6.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
            xaxis_title="Процентная ставка (%)",
            yaxis_title="Количество расчетов",
            autosize=False
        )
        st.plotly_chart(fig6, use_container_width=True, key="interest_rates")
    
    # Тепловая карта активности по часам и дням недели
    st.write("**🕐 Тепловая карта активности (час/день недели)**")
    
    # Подготовка данных для тепловой карты
    activity_pivot = df_filtered.pivot_table(
        values='id', 
        index='hour_only', 
        columns='day_of_week', 
        aggfunc='count', 
        fill_value=0
    )
    
    # Названия дней недели
    day_names = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
    activity_pivot.columns = [day_names[int(col)] for col in activity_pivot.columns]
    
    fig7 = px.imshow(
        activity_pivot.T,  # Транспонируем для удобства
        labels=dict(x="Час", y="День недели", color="Количество расчетов"),
        aspect="auto",
        color_continuous_scale='viridis'
    )
    fig7.update_layout(
        height=300, 
        margin=dict(l=20, r=20, t=20, b=20),
        autosize=False
    )
    st.plotly_chart(fig7, use_container_width=True, key="heatmap")
    
    # Таблица топ клиентов
    st.subheader("Топ активных пользователей")
    
    top_clients = df_filtered.groupby('client_id').agg({
        'id': 'count',
        'initial_sum': 'mean',
        'final_amount': 'mean',
        'created_at': 'max'
    }).round(0).sort_values('id', ascending=False).head(10)
    
    top_clients.columns = ['Количество расчетов', 'Средняя сумма', 'Средний результат', 'Последний расчет']
    
    # Форматирование для отображения
    top_clients_display = top_clients.copy()
    top_clients_display['Средняя сумма'] = top_clients_display['Средняя сумма'].apply(
        lambda x: f"{x/1000000:.1f}М ₽"
    )
    top_clients_display['Средний результат'] = top_clients_display['Средний результат'].apply(
        lambda x: f"{x/1000000:.1f}М ₽"
    )
    top_clients_display['Последний расчет'] = pd.to_datetime(top_clients_display['Последний расчет']).dt.strftime('%d.%m.%Y %H:%M')
    
    st.dataframe(top_clients_display, use_container_width=True)
    
    # Статистика производительности
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Производительность API")
        avg_response = df_filtered['api_response_time_ms'].mean()
        median_response = df_filtered['api_response_time_ms'].median()
        max_response = df_filtered['api_response_time_ms'].max()
        
        perf_metrics = pd.DataFrame({
            'Метрика': ['Среднее время', 'Медианное время', 'Максимальное время'],
            'Значение (ms)': [avg_response, median_response, max_response]
        })
        
        fig8 = px.bar(
            perf_metrics, 
            x='Метрика', 
            y='Значение (ms)',
            color='Значение (ms)',
            color_continuous_scale='RdYlGn_r'
        )
        fig8.update_layout(
            height=300,
            showlegend=False,
            margin=dict(l=20, r=20, t=20, b=20),
            autosize=False
        )
        st.plotly_chart(fig8, use_container_width=True, key="performance")
    
    with col2:
        st.subheader("Сводная статистика")
        
        summary_stats = pd.DataFrame({
            'Метрика': [
                'Общий объем инвестиций',
                'Общая прибыль',
                'Средний период (лет)',
                'Популярная валюта'
            ],
            'Значение': [
                f"{df_filtered['initial_sum'].sum()/1000000000:.1f}млрд ₽",
                f"{df_filtered['total_profit'].sum()/1000000000:.1f}млрд ₽", 
                f"{df_filtered[df_filtered['period_unit']=='y']['period'].mean():.1f}",
                df_filtered['currency'].value_counts().index[0]
            ]
        })
        
        # Стилизованная таблица
        st.dataframe(
            summary_stats.set_index('Метрика'),
            use_container_width=True
        )
    
    # Информация об обновлении
    # Курсы валют внизу боковой панели
    st.sidebar.markdown("---")
    st.sidebar.subheader("💱 Курсы ЦБ РФ")
    
    # Получаем курсы валют
    rates = get_cbr_rates()
    
    # Отображаем курсы в компактном формате
    currency_info = []
    for curr, rate in rates.items():
        if curr != 'RUB':
            currency_info.append(f"**{curr}:** {rate:.2f}₽")
    
    currency_string = " • ".join(currency_info)
    st.sidebar.markdown(currency_string)
    
    st.sidebar.markdown("---")
    st.sidebar.info(f"Последнее обновление: {datetime.now().strftime('%H:%M:%S')}")
    st.sidebar.markdown(f"**Всего записей в БД:** {len(df):,}")
    st.sidebar.markdown(f"**Период данных:** {df['date_only'].min().strftime('%d.%m.%Y')} - {df['date_only'].max().strftime('%d.%m.%Y')}")

if __name__ == "__main__":
    main() 