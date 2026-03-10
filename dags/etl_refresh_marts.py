from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2


POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "etl_db"
POSTGRES_USER = "levon"
POSTGRES_PASSWORD = "12345"


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def check_postgres_connection() -> None:
    conn = get_postgres_connection()
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


def refresh_user_activity_mart() -> None:
    conn = get_postgres_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("REFRESH MATERIALIZED VIEW etl.user_activity_mart;")
                print("Materialized view etl.user_activity_mart refreshed successfully.")
    except Exception as e:
        print(f"Failed to refresh etl.user_activity_mart: {e}")
        raise
    finally:
        conn.close()


def refresh_support_efficiency_mart() -> None:
    conn = get_postgres_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("REFRESH MATERIALIZED VIEW etl.support_efficiency_mart;")
                print("Materialized view etl.support_efficiency_mart refreshed successfully.")
    except Exception as e:
        print(f"Failed to refresh etl.support_efficiency_mart: {e}")
        raise
    finally:
        conn.close()


default_args = {
    "owner": "levon",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="refresh_marts",
    default_args=default_args,
    description="Refresh analytical marts in PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "marts", "postgres"],
) as dag:

    check_postgres_task = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres_connection,
    )

    refresh_user_activity_task = PythonOperator(
        task_id="refresh_user_activity_mart",
        python_callable=refresh_user_activity_mart,
    )

    refresh_support_efficiency_task = PythonOperator(
        task_id="refresh_support_efficiency_mart",
        python_callable=refresh_support_efficiency_mart,
    )

    check_postgres_task >> [refresh_user_activity_task, refresh_support_efficiency_task]