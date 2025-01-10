'''
Утилиты для домашней работы
'''
import pymysql
from pymysql import Error
import zipfile
import os
from sqlalchemy import create_engine, text, inspect
import pandas as pd

#--------------------------------------------------------
def unpack_zip(zip_file_path, output_folder='data'): 
    '''
    Функция распаковывает архив в папку
    '''
    os.makedirs(output_folder, exist_ok=True) # Создаем папку, если она не существует
    
    
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref: # Открываем ZIP-архив
        zip_ref.extractall(output_folder) # Извлекаем все содержимое в указанную папку
    
    print(f'Содержимое {zip_file_path} успешно распаковано в папку {output_folder}.')

# if __name__ in '__main__': # Проверка функции unpack_zip
#     unpack_zip('archive.zip', 'data')

#--------------------------------------------------------
def manager_db(database_name, db_action = 'add',  **db_config):
    '''
    функция создает или удаляет БД MySQL
    database_name - имя БД
    db_action  - 'add' - создание, 'del' - удаление
    db_config = {
         'host': host, # хост
         'user': user, # имя пользователя
         'password': password # пароль
     }    
    '''   
    try:        
        connection = pymysql.connect(**db_config) # Устанавливаем соединение c БД
        if connection.open:
            print('Соединение установлено')            
            cursor = connection.cursor() # Создаем курсор          
            cursor.execute("SHOW DATABASES") # Запрашиваем список баз данных
            databases = cursor.fetchall() # Извлекаем весь курсор            
            existing_databases = [db[0] for db in databases] # # Получаем список имен баз данных
            if db_action == 'add': # если создание БД
                if database_name not in existing_databases: # Проверяем отсутсвие БД с требуемым имененем на сервере
                    cursor.execute(f"CREATE DATABASE {database_name}") # Если нет, создаем БД
                    print(f'База данных "{database_name}" успешно создана.')
                else: # Если есть информируем
                    print(f'База данных "{database_name}" уже существует.')
            if db_action == 'del': # если удаление БД
                if database_name in existing_databases: # Проверяем на наличие БД с таким именем
                    cursor.execute(f"DROP DATABASE {database_name}") # Удаляем БД
                    print(f'База данных "{database_name}" успешно удалена.')
                else:
                    print(f'База данных "{database_name}" не существует.')
    except Error as e: # Выводим ошибку в случе неудачного подключения
        print(f'Произошла ошибка при соединении: {e}')

    finally:
        if 'cursor' in locals(): # Проверяем создана ли переменная cursor 
            cursor.close() # Закрываем курсор
        if connection and connection.open: # Проверяем наличие соединения
            connection.close() # Закрываем соединение
            print('Соединение закрыто')

# if __name__ in '__main__': # Проверка функции manager_db
#      # Параметры подключения к базе данных
#     db_config = {
#         'host': 'localhost', # хост
#         'user': 'root', # имя пользователя
#         'password': 'parol' # пароль
#     }    
#     manager_db('bike_store', 'del', **db_config)

#--------------------------------------------------------
def import_data(database_name, data_folder = 'data', **db_config):
    ''' 
    Функция создает таблицы, заполняет их данными, назначает первичные ключи
    и устанавливает связи между таблицами на основании файлов с расширением 
    .csv в указанной директории
    '''
    # Подключение к базе данных MySQL
    connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{database_name}"
    engine = create_engine(connection_string)

    # Получаем список всех файлов с расширением .csv в директории
    csv_files = [filename for filename in os.listdir(data_folder) if filename.endswith('.csv')]

    # Цикл по всем файлам .csv
    for csv_file in csv_files:
        full_path = os.path.join(data_folder, csv_file)  # Формируем полный путь к файлу
        
        # Читаем данные из CSV файла
        df = pd.read_csv(full_path)
        
        # Создаем таблицу в базе данных с именем файла (без расширения)
        table_name = os.path.splitext(csv_file)[0]
        
        # Заполняем таблицу данными
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        
    # Создание первичных ключей (используется первое поле, для таблиц order_items', 'stocks' создается составной ключ из первого и второго поля)
    with engine.connect() as connection:
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        for table_name in tables:
            # Получаем информацию о столбцах таблицы
            columns = inspector.get_columns(table_name)
        
            # Назначаем первичный ключ на первое поле
            primary_key_column = columns[0]['name']
            
            # Проверяем, является ли таблица order_items или stocks
            if table_name in ['order_items', 'stocks']:
                # Если это order_items или stocks, назначаем составной первичный ключ
                primary_key_columns = [columns[0]['name'], columns[1]['name']]
                alter_table_query = f'ALTER TABLE {table_name} ADD PRIMARY KEY ({", ".join(primary_key_columns)});'
            else:
                # Для остальных таблиц
                alter_table_query = f'ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key_column});'

            # Выполнение запроса
            try:
                connection.execute(text(alter_table_query))
                print(f'Первичный ключ для таблицы {table_name} успешно создан')
            except Exception as e:
                print(f'Ошибка при назначении первичного ключа для таблицы {table_name}: {e}')
                
    # Создание внешних связей
    with engine.connect() as connection:
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        # Словарь для хранения первичных ключей и их таблиц
        primary_keys = {}

        # Проходим по всем таблицам и собираем первичные ключи
        for table_name in tables:
            pk_constraint = inspector.get_pk_constraint(table_name)
            primary_key_columns = pk_constraint['constrained_columns']
            
            # Пропускаем таблицы с составными ключами 
            if len(primary_key_columns) == 1:
                primary_keys[table_name] = primary_key_columns

    # Устанавливаем связи и каскадное удаление
        for table_name, pk_columns in primary_keys.items():
            for pk_column in pk_columns:
                # Ищем этот первичный ключ в других таблицах
                for other_table in tables:
                    if other_table != table_name: 
                        other_columns = inspector.get_columns(other_table)
                        for column in other_columns:
                            if column['name'] == pk_column:
                                # Создаем внешний ключ с каскадным удалением
                                foreign_key_query = f'''
                                ALTER TABLE {other_table}
                                ADD CONSTRAINT fk_{other_table}_{table_name}
                                FOREIGN KEY ({pk_column}) REFERENCES {table_name}({pk_column})
                                ON DELETE CASCADE;
                                '''
                                try:
                                    connection.execute(text(foreign_key_query))
                                    print(f'Связь установлена между {table_name} и {other_table} по полю {pk_column}')
                                except Exception as e:
                                    print(f'Ошибка при установке связи между {table_name} и {other_table}: {e}')

# if __name__ in '__main__': # Проверка функции 
#      # Параметры подключения к базе данных
#     db_config = {
#         'host': 'localhost', # хост
#         'user': 'root', # имя пользователя
#         'password': 'parol' # пароль
#     }    
#     manager_db('bike_store', 'del', **db_config)# Удаляем базу 
#     manager_db('bike_store', 'add', **db_config)# Создаем базу
#     import_data('bike_store', 'data', **db_config)

#--------------------------------------------------------
