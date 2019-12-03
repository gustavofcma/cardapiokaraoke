import os
import csv
import sqlite3

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), 'karaoke.sqlite3')
DEFAULT_CSV_PATH = os.path.join(os.path.dirname(__file__), 'karaoke.csv')


def connect_db(db_path=DEFAULT_DB_PATH):
    connection = sqlite3.connect(db_path)
    print('Connection successful!')

    query = 'select sqlite_version();'
    cursor = connection.cursor()
    cursor.execute(query)
    record = cursor.fetchall()
    print(f'DB Version: {record[0][0]}')
    cursor.close()

    return connection


def create_schema(connection):
    drop = 'DROP TABLE IF EXISTS musicas;'
    statement = '''
    CREATE TABLE IF NOT EXISTS musicas (
        codigo INTEGER UNIQUE,
        artista TEXT,
        titulo TEXT,
        inicio TEXT,
        pacotes TEXT,
    PRIMARY KEY(codigo)
);'''

    cursor = connection.cursor()
    cursor.execute(drop)
    cursor.execute(statement)
    connection.commit()


def load_csv(arquivo=DEFAULT_CSV_PATH):
    linhas = []
    with open(arquivo) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        for row in csv_reader:
            linhas.append(row)

    return linhas


def insert_data(item_list, connection):
    cursor = connection.cursor()
    item_list = item_list[1:]
    insert_tables = 'INSERT INTO musicas (artista, codigo, titulo, inicio, pacotes) VALUES (?, ?, ?, ?, ?)'
    cursor.executemany(insert_tables, item_list)
    connection.commit()


def main():
    conn = connect_db()
    create_schema(conn)
    musicas = load_csv()
    print(musicas[2])
    insert_data(musicas, conn)
    conn.close()


if __name__ == '__main__':
    main()
