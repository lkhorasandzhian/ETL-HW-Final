from datetime import datetime, timedelta, timezone
from random import choice, randint, sample
from faker import Faker
from pymongo import MongoClient

fake = Faker("ru_RU")

MONGO_URI = "mongodb://mongo:27017/"
DB_NAME = "etl_mongo"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

user_sessions_col = db["UserSessions"]
event_logs_col = db["EventLogs"]
support_tickets_col = db["SupportTickets"]
user_recommendations_col = db["UserRecommendations"]
moderation_queue_col = db["ModerationQueue"]


def random_datetime_last_days(days: int = 90) -> datetime:
    now = datetime.now(timezone.utc)
    delta = timedelta(
        days=randint(0, days),
        hours=randint(0, 23),
        minutes=randint(0, 59),
        seconds=randint(0, 59),
    )
    return now - delta


def generate_users(n: int) -> list[str]:
    return [f"user_{i:05d}" for i in range(1, n + 1)]


def generate_products(n: int) -> list[str]:
    return [f"prod_{i:05d}" for i in range(1, n + 1)]


def generate_pages(products: list[str]) -> list[str]:
    static_pages = [
        "/home",
        "/products",
        "/cart",
        "/checkout",
        "/profile",
        "/orders",
        "/support",
    ]
    product_pages = [f"/products/{p}" for p in products]
    return static_pages + product_pages


def generate_user_sessions(users: list[str], products: list[str], n: int) -> list[dict]:
    actions_pool = [
        "login",
        "logout",
        "view_product",
        "add_to_cart",
        "remove_from_cart",
        "search",
        "checkout_started",
        "purchase",
        "open_support",
        "add_to_favorites",
    ]
    pages = generate_pages(products)

    docs = []
    for i in range(1, n + 1):
        start_time = random_datetime_last_days()
        duration_minutes = randint(3, 90)
        end_time = start_time + timedelta(minutes=duration_minutes)

        visited_count = randint(2, 6)
        pages_visited = sample(pages, k=min(visited_count, len(pages)))

        actions_count = randint(2, 5)
        actions = sample(actions_pool, k=actions_count)

        docs.append(
            {
                "session_id": f"sess_{i:06d}",
                "user_id": choice(users),
                "start_time": start_time,
                "end_time": end_time,
                "pages_visited": pages_visited,
                "device": choice(["mobile", "desktop", "web"]),
                "actions": actions,
            }
        )
    return docs


def generate_event_logs(products: list[str], n: int) -> list[dict]:
    event_types = ["click", "view", "search", "purchase", "login", "logout"]

    pages_for_details = [
        "/home",
        "/products",
        "/cart",
        "/checkout",
    ] + [f"/products/{p}" for p in products]

    docs = []
    for i in range(1, n + 1):
        docs.append(
            {
                "event_id": f"evt_{i:06d}",
                "timestamp": random_datetime_last_days(),
                "event_type": choice(event_types),
                "details": choice(pages_for_details),
            }
        )
    return docs


def generate_support_tickets(users: list[str], n: int) -> list[dict]:
    statuses = ["open", "in_progress", "resolved", "closed"]
    issue_types = ["payment", "delivery", "refund", "account", "technical", "product_issue"]

    docs = []
    for i in range(1, n + 1):
        created_at = random_datetime_last_days()
        msg_count = randint(1, 4)
        messages = []

        current_ts = created_at
        for msg_idx in range(msg_count):
            current_ts += timedelta(minutes=randint(5, 180))
            sender = "user" if msg_idx % 2 == 0 else "support"
            messages.append(
                {
                    "sender": sender,
                    "message": fake.sentence(nb_words=8),
                    "timestamp": current_ts,
                }
            )

        updated_at = messages[-1]["timestamp"] if messages else created_at

        docs.append(
            {
                "ticket_id": f"ticket_{i:06d}",
                "user_id": choice(users),
                "status": choice(statuses),
                "issue_type": choice(issue_types),
                "messages": messages,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )
    return docs


def generate_user_recommendations(users: list[str], products: list[str], n: int) -> list[dict]:
    docs = []
    selected_users = sample(users, k=min(n, len(users)))
    for user_id in selected_users:
        docs.append(
            {
                "user_id": user_id,
                "recommended_products": sample(products, k=randint(3, 7)),
                "last_updated": random_datetime_last_days(),
            }
        )
    return docs


def generate_moderation_queue(users: list[str], products: list[str], n: int) -> list[dict]:
    moderation_statuses = ["pending", "approved", "rejected"]
    flags_pool = ["contains_images", "spam_suspected", "toxicity_check", "duplicate_review"]

    docs = []
    for i in range(1, n + 1):
        flags_count = randint(0, 2)
        flags = sample(flags_pool, k=flags_count) if flags_count > 0 else []

        docs.append(
            {
                "review_id": f"rev_{i:06d}",
                "user_id": choice(users),
                "product_id": choice(products),
                "review_text": fake.text(max_nb_chars=180),
                "rating": randint(1, 5),
                "moderation_status": choice(moderation_statuses),
                "flags": flags,
                "submitted_at": random_datetime_last_days(),
            }
        )
    return docs


def recreate_collections() -> None:
    db.drop_collection("UserSessions")
    db.drop_collection("EventLogs")
    db.drop_collection("SupportTickets")
    db.drop_collection("UserRecommendations")
    db.drop_collection("ModerationQueue")


def create_indexes() -> None:
    user_sessions_col.create_index("session_id", unique=True)
    user_sessions_col.create_index("user_id")
    user_sessions_col.create_index("start_time")

    event_logs_col.create_index("event_id", unique=True)
    event_logs_col.create_index("timestamp")
    event_logs_col.create_index("event_type")

    support_tickets_col.create_index("ticket_id", unique=True)
    support_tickets_col.create_index("user_id")
    support_tickets_col.create_index("status")

    user_recommendations_col.create_index("user_id", unique=True)
    user_recommendations_col.create_index("last_updated")

    moderation_queue_col.create_index("review_id", unique=True)
    moderation_queue_col.create_index("product_id")
    moderation_queue_col.create_index("moderation_status")


def main():
    print("Recreating collections...")
    recreate_collections()

    users = generate_users(2000)
    products = generate_products(800)

    print("Generating documents...")
    user_sessions = generate_user_sessions(users, products, 3000)
    event_logs = generate_event_logs(products, 5000)
    support_tickets = generate_support_tickets(users, 1000)
    user_recommendations = generate_user_recommendations(users, products, 1500)
    moderation_queue = generate_moderation_queue(users, products, 1200)

    print("Inserting documents into MongoDB...")
    user_sessions_col.insert_many(user_sessions)
    event_logs_col.insert_many(event_logs)
    support_tickets_col.insert_many(support_tickets)
    user_recommendations_col.insert_many(user_recommendations)
    moderation_queue_col.insert_many(moderation_queue)

    print("Creating indexes...")
    create_indexes()

    print("Done.")
    print(f"UserSessions: {user_sessions_col.count_documents({})}")
    print(f"EventLogs: {event_logs_col.count_documents({})}")
    print(f"SupportTickets: {support_tickets_col.count_documents({})}")
    print(f"UserRecommendations: {user_recommendations_col.count_documents({})}")
    print(f"ModerationQueue: {moderation_queue_col.count_documents({})}")


if __name__ == "__main__":
    main()