from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values


MONGO_URI = "mongodb://mongo:27017/"
MONGO_DB_NAME = "etl_mongo"

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "etl_db"
POSTGRES_USER = "levon"
POSTGRES_PASSWORD = "12345"


def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB_NAME]


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def truncate_tables(cursor) -> None:
    cursor.execute(
        """
        TRUNCATE TABLE
            etl.user_session_pages,
            etl.user_session_actions,
            etl.event_logs,
            etl.support_ticket_messages,
            etl.support_tickets,
            etl.user_recommendation_products,
            etl.user_recommendations,
            etl.moderation_queue_flags,
            etl.moderation_queue,
            etl.user_sessions
        RESTART IDENTITY CASCADE;
        """
    )


def load_user_sessions(mongo_db, cursor) -> None:
    documents = list(mongo_db["UserSessions"].find())

    user_sessions_rows = []
    user_session_pages_rows = []
    user_session_actions_rows = []

    for doc in documents:
        session_id = doc["session_id"]

        user_sessions_rows.append(
            (
                session_id,
                doc["user_id"],
                doc["start_time"],
                doc["end_time"],
                doc.get("device"),
            )
        )

        for page in doc.get("pages_visited", []):
            user_session_pages_rows.append((session_id, page))

        for action in doc.get("actions", []):
            user_session_actions_rows.append((session_id, action))

    if user_sessions_rows:
        execute_values(
            cursor,
            """
            INSERT INTO etl.user_sessions (
                session_id,
                user_id,
                start_time,
                end_time,
                device
            ) VALUES %s
            """,
            user_sessions_rows,
        )

    if user_session_pages_rows:
        execute_values(
            cursor,
            """
            INSERT INTO etl.user_session_pages (
                session_id,
                page_path
            ) VALUES %s
            """,
            user_session_pages_rows,
        )

    if user_session_actions_rows:
        execute_values(
            cursor,
            """
            INSERT INTO etl.user_session_actions (
                session_id,
                action_name
            ) VALUES %s
            """,
            user_session_actions_rows,
        )

    print(f"Loaded UserSessions: {len(user_sessions_rows)}")
    print(f"Loaded UserSessionPages: {len(user_session_pages_rows)}")
    print(f"Loaded UserSessionActions: {len(user_session_actions_rows)}")


def load_event_logs(mongo_db, cursor) -> None:
    documents = list(mongo_db["EventLogs"].find())

    rows = []
    for doc in documents:
        rows.append(
            (
                doc["event_id"],
                doc["timestamp"],
                doc["event_type"],
                doc.get("details"),
            )
        )

    if rows:
        execute_values(
            cursor,
            """
            INSERT INTO etl.event_logs (
                event_id,
                event_timestamp,
                event_type,
                details
            ) VALUES %s
            """,
            rows,
        )

    print(f"Loaded EventLogs: {len(rows)}")


def load_support_tickets(mongo_db, cursor) -> None:
    documents = list(mongo_db["SupportTickets"].find())

    support_tickets_rows = []
    support_ticket_messages_rows = []

    for doc in documents:
        ticket_id = doc["ticket_id"]

        support_tickets_rows.append(
            (
                ticket_id,
                doc["user_id"],
                doc["status"],
                doc["issue_type"],
                doc["created_at"],
                doc["updated_at"],
            )
        )

        for message in doc.get("messages", []):
            support_ticket_messages_rows.append(
                (
                    ticket_id,
                    message["sender"],
                    message["message"],
                    message["timestamp"],
                )
            )

    if support_tickets_rows:
        execute_values(
            cursor,
            """
            INSERT INTO etl.support_tickets (
                ticket_id,
                user_id,
                status,
                issue_type,
                created_at,
                updated_at
            ) VALUES %s
            """,
            support_tickets_rows,
        )

    if support_ticket_messages_rows:
        execute_values(
            cursor,
            """
            INSERT INTO etl.support_ticket_messages (
                ticket_id,
                sender,
                message_text,
                message_timestamp
            ) VALUES %s
            """,
            support_ticket_messages_rows,
        )

    print(f"Loaded SupportTickets: {len(support_tickets_rows)}")
    print(f"Loaded SupportTicketMessages: {len(support_ticket_messages_rows)}")


def load_user_recommendations(mongo_db, cursor) -> None:
    documents = list(mongo_db["UserRecommendations"].find())

    user_recommendations_rows = []
    user_recommendation_products_rows = []

    for doc in documents:
        user_id = doc["user_id"]

        user_recommendations_rows.append(
            (
                user_id,
                doc["last_updated"],
            )
        )

        for product_id in doc.get("recommended_products", []):
            user_recommendation_products_rows.append((user_id, product_id))

    if user_recommendations_rows:
        execute_values(
            cursor,
            """
            INSERT INTO etl.user_recommendations (
                user_id,
                last_updated
            ) VALUES %s
            """,
            user_recommendations_rows,
        )

    if user_recommendation_products_rows:
        execute_values(
            cursor,
            """
            INSERT INTO etl.user_recommendation_products (
                user_id,
                product_id
            ) VALUES %s
            """,
            user_recommendation_products_rows,
        )

    print(f"Loaded UserRecommendations: {len(user_recommendations_rows)}")
    print(f"Loaded UserRecommendationProducts: {len(user_recommendation_products_rows)}")


def load_moderation_queue(mongo_db, cursor) -> None:
    documents = list(mongo_db["ModerationQueue"].find())

    moderation_queue_rows = []
    moderation_queue_flags_rows = []

    for doc in documents:
        review_id = doc["review_id"]

        moderation_queue_rows.append(
            (
                review_id,
                doc["user_id"],
                doc["product_id"],
                doc.get("review_text"),
                doc["rating"],
                doc["moderation_status"],
                doc["submitted_at"],
            )
        )

        for flag in doc.get("flags", []):
            moderation_queue_flags_rows.append((review_id, flag))

    if moderation_queue_rows:
        execute_values(
            cursor,
            """
            INSERT INTO etl.moderation_queue (
                review_id,
                user_id,
                product_id,
                review_text,
                rating,
                moderation_status,
                submitted_at
            ) VALUES %s
            """,
            moderation_queue_rows,
        )

    if moderation_queue_flags_rows:
        execute_values(
            cursor,
            """
            INSERT INTO etl.moderation_queue_flags (
                review_id,
                flag_name
            ) VALUES %s
            """,
            moderation_queue_flags_rows,
        )

    print(f"Loaded ModerationQueue: {len(moderation_queue_rows)}")
    print(f"Loaded ModerationQueueFlags: {len(moderation_queue_flags_rows)}")


def validate_counts(mongo_db, cursor) -> None:
    checks: list[tuple[str, int, str]] = [
        ("UserSessions", mongo_db["UserSessions"].count_documents({}), "SELECT COUNT(*) FROM etl.user_sessions"),
        ("EventLogs", mongo_db["EventLogs"].count_documents({}), "SELECT COUNT(*) FROM etl.event_logs"),
        ("SupportTickets", mongo_db["SupportTickets"].count_documents({}), "SELECT COUNT(*) FROM etl.support_tickets"),
        (
            "UserRecommendations",
            mongo_db["UserRecommendations"].count_documents({}),
            "SELECT COUNT(*) FROM etl.user_recommendations",
        ),
        ("ModerationQueue", mongo_db["ModerationQueue"].count_documents({}), "SELECT COUNT(*) FROM etl.moderation_queue"),
    ]

    print("\nValidation:")
    for name, mongo_count, sql_query in checks:
        cursor.execute(sql_query)
        postgres_count = cursor.fetchone()[0]
        print(f"{name}: MongoDB={mongo_count}, PostgreSQL={postgres_count}")


def main():
    mongo_db = get_mongo_db()
    conn = get_postgres_connection()

    try:
        with conn:
            with conn.cursor() as cursor:
                print("Truncating PostgreSQL tables...")
                truncate_tables(cursor)
                print("Loading UserSessions...")
                load_user_sessions(mongo_db, cursor)
                print("Loading EventLogs...")
                load_event_logs(mongo_db, cursor)
                print("Loading SupportTickets...")
                load_support_tickets(mongo_db, cursor)
                print("Loading UserRecommendations...")
                load_user_recommendations(mongo_db, cursor)
                print("Loading ModerationQueue...")
                load_moderation_queue(mongo_db, cursor)
                validate_counts(mongo_db, cursor)
        print("\nETL replication completed successfully.")
    except Exception as e:
        print(f"ETL replication failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()