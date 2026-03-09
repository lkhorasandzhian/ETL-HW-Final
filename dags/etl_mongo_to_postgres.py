from datetime import datetime
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import psycopg2


MONGO_URI = "mongodb://mongo:27017/"
MONGO_DB_NAME = "etl_mongo"

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "etl_db"
POSTGRES_USER = "levon"
POSTGRES_PASSWORD = "12345"


def check_mongo_connection() -> None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db_names = client.list_database_names()
    print("MongoDB is available.")
    print(f"Databases: {db_names}")

    if MONGO_DB_NAME not in db_names:
        print(f"Warning: database '{MONGO_DB_NAME}' does not exist yet.")


def check_postgres_connection() -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
            result = cursor.fetchone()
            print("PostgreSQL is available.")
            print(f"Test query result: {result}")
    except Exception as e:
        print(f"PostgreSQL connection failed: {e}")
        raise
    finally:
        conn.close()


def run_etl_script() -> None:
    result = subprocess.run(
        ["python", "/opt/airflow/scripts/etl_pipeline.py"],
        check=True,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        print("STDOUT:")
        print(result.stdout)

    if result.stderr:
        print("STDERR:")
        print(result.stderr)


default_args = {
    "owner": "levon",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="etl_mongo_to_postgres",
    default_args=default_args,
    description="ETL replication from MongoDB to PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "mongo", "postgres"],
) as dag:

    check_mongo_task = PythonOperator(
        task_id="check_mongo_connection",
        python_callable=check_mongo_connection,
    )

    check_postgres_task = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres_connection,
    )

    run_etl_task = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl_script,
    )

    [check_mongo_task, check_postgres_task] >> run_etl_task