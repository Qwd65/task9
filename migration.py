from clickhouse_driver import Client
import psycopg2

pg_conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port=5432
)
pg_cursor = pg_conn.cursor()

clickhouse_client = Client(host='localhost',port=8123)

pg_cursor.execute("SELECT id, name, age, salary FROM users")
rows = pg_cursor.fetchall()

insert_query = 'INSERT INTO users (id, name, age, salary) VALUES'
clickhouse_client.execute(insert_query, rows)

pg_cursor.close()
pg_conn.close()
