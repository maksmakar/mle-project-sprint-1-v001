# dags/churn.py
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

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message, 
    on_failure_callback=send_telegram_failure_message
)
def prediction_price_etl():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    @task()
    def create_table(**kwargs):
        print(PLUGIN_PATH)
        import sqlalchemy
        from sqlalchemy import (MetaData, Table, 
                                Column, String, Integer, Numeric,
                                Float, Boolean, 
                                DateTime, UniqueConstraint, inspect)
        
        postgres_hook = PostgresHook('destination_db')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        metadata = MetaData()
        prediction_price_table = Table(
            'prediction_price_estate',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('floor', Integer),
            Column('is_apartment', Boolean),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('studio', Boolean),
            Column('total_area', Float),
            Column('price', Numeric),
            Column('building_id', String),
            Column('build_year', String),
            Column('building_type_int', String),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            UniqueConstraint('id', name='unique_flat_id_constraint')
        )
        if not inspect(engine).has_table(prediction_price_table.name):
            metadata.create_all(engine)
        
    @task()
    def extract(**kwargs):
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        
        buildings = pd.read_sql("""Select*from buildings """, conn)
        flats = pd.read_sql("""Select*from flats """, conn)

        data = flats.merge(buildings, how='left', left_on='building_id', right_on ='id')

        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        # убираем ненужное после джойна табличек    
        data.rename(columns={'id_x': 'id'}, inplace=True)
        data.drop('id_y', axis=1, inplace=True)

        data['price'] = data['price'].astype('int64')

        # ставлю более уместный тип данных для некоторых полей (категории)
        data['building_id'] = data['building_id'].astype('object')
        data['build_year'] = data['build_year'].astype('object')
        data['building_type_int'] = data['building_type_int'].astype('object')

        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="prediction_price_estate",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['id'],
            rows=data.values.tolist()
    )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
prediction_price_etl()


