## scripts/data.py

# 1 — импорты
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import yaml

dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))

# 2 — вспомогательные функции
def create_connection(dotenv_path=None):

    load_dotenv(dotenv_path)
    host = os.environ.get('DB_DESTINATION_HOST')
    port = os.environ.get('DB_DESTINATION_PORT')
    db = os.environ.get('DB_DESTINATION_NAME')
    username = os.environ.get('DB_DESTINATION_USER')
    password = os.environ.get('DB_DESTINATION_PASSWORD')
    
    print(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}', connect_args={'sslmode':'require'})
    return conn

# 3 — главная функция
def get_data():
    # 3.1 — загрузка гиперпараметров
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    # 3.2 — загрузки предыдущих результатов нет, так как это первый шаг

    # 3.3 — основная логика
    conn = create_connection()
    data = pd.read_sql('select * from clean_prediction_price_estate', conn, index_col=params['index_col'])
    conn.dispose()

    # 3.4 — сохранение результата шага
    os.makedirs('part2_dvc/data', exist_ok=True)
    data.to_csv('part2_dvc/data/initial_data.csv', index=None)

# 4 — защищённый вызов главной функции
if __name__ == '__main__':
    get_data()