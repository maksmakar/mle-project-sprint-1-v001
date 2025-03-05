import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator 

from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine

import sys
import os

PLUGIN_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if PLUGIN_PATH not in sys.path:
    sys.path.append(PLUGIN_PATH) 

from plugins.steps.messages import send_telegram_success_message, send_telegram_failure_message 

print(PLUGIN_PATH)

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"],
    on_success_callback=send_telegram_success_message, 
    on_failure_callback=send_telegram_failure_message
)
def clean_prediction_price_etl():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    @task()
    def create_table():

        from sqlalchemy import (Table, Column, DateTime, Float, BigInteger,
                                Integer, Index, MetaData, Boolean,
                                String, UniqueConstraint, inspect)
        
        hook = PostgresHook('destination_db')
        db_engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()

        clean_prediction_price_table = Table(
            'clean_prediction_price_estate',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('floor', Integer),
            Column('is_apartment', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('total_area', Float),
            Column('price', BigInteger),
            Column('building_id', String),
            Column('build_year', String),
            Column('building_type_int', String),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Integer), 
            UniqueConstraint('id', name='clean_unique_flat_id_constraint')
        )
        if not inspect(db_engine).has_table(clean_prediction_price_table.name):
            metadata.create_all(db_engine)

    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = " Select*from prediction_price_estate"
        
        data = pd.read_sql(sql, conn)
        conn.close()

        return data

    @task()
    def transform(data: pd.DataFrame):
    
        # Избавляемся от дубликатов
        feature_cols = data.columns.drop('id').tolist()
        is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
        data = data[~is_duplicated_features].reset_index(drop=True)
        # Столбец studio содержит только одно значение, поэтому он будет бесполезен при обучении
        data.drop('studio', axis=1, inplace=True)

        # заменим булевы типы данных на 0 и 1
        bools = data.select_dtypes(['bool']).columns 
        for col in bools:
            data[col] = data[col].astype(int)

        # На всякий случай делаем замену пустых значений (хотя конкретно в этом случае пустых полей не было)
        cols_with_nans = data.isnull().sum()
        cols_with_nans = cols_with_nans[cols_with_nans > 0].index

        for col in cols_with_nans:
            if data[col].dtype in [float, int]:
                fill_value = data[col].mean()

            elif data[col].dtype == 'object':
                fill_value = data[col].mode().iloc[0]
            data[col] = data[col].fillna(fill_value)

        num_cols = data.select_dtypes(['float']).columns  # Выбираем только числовые колонки типа float
        threshold = 1.5  # Коэффициент для метода IQR
        potential_outliers = pd.DataFrame()

        for col in num_cols:
            Q1 = data[col].quantile(0.25)  # Первый квартиль (25%)
            Q3 = data[col].quantile(0.75)  # Третий квартиль (75%)
            IQR = Q3 - Q1  # Межквартильный размах
            margin = IQR * threshold  # Определяем границы выбросов
            lower = Q1 - margin  # Нижняя граница
            upper = Q3 + margin  # Верхняя граница
            potential_outliers[col] = ~data[col].between(lower, upper)  # True для выбросов

        outliers = potential_outliers.any(axis=1)  # Определяем строки, где есть хотя бы один выброс
        data = data[~outliers].reset_index(drop=True)

        # Не смотря на то, что price является таргетом, обучаться на выбросах не очень хорошо, поэтому убрали строки с выбросами и по этому столбцу тоже

        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')

        hook.insert_rows(
            table= 'clean_prediction_price_estate',
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['id'],
            rows=data.values.tolist()
    )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_prediction_price_etl()
