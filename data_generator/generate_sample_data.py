# Databricks notebook source

# MAGIC %md
# MAGIC # Sample Data Generator
# MAGIC
# MAGIC Generates realistic test data for all source systems in the Customer 360 platform.
# MAGIC Run this notebook first to populate the landing zone with sample files.
# MAGIC
# MAGIC **Output**: JSON, CSV, and Parquet files written to the landing zone directories.

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, DateType, ArrayType, BooleanType
)

# COMMAND ----------

# Configuration
NUM_CUSTOMERS = 1000
NUM_TRANSACTIONS = 10000
NUM_CLICKSTREAM_EVENTS = 50000
NUM_SUPPORT_TICKETS = 500
LANDING_PATH = "/Volumes/customer_360_catalog/landing/raw_data"

# Seed for reproducibility
random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Data

# COMMAND ----------

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen",
    "Charles", "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony",
    "Margaret", "Mark", "Sandra", "Donald", "Ashley", "Steven", "Kimberly",
    "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson",
]

CITIES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Philadelphia", "PA"),
    ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"),
    ("San Jose", "CA"), ("Austin", "TX"), ("Jacksonville", "FL"),
    ("Denver", "CO"), ("Seattle", "WA"), ("Boston", "MA"),
]

LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]
PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Books",
                      "Sports", "Beauty", "Toys", "Food & Beverage"]
PRODUCT_NAMES = {
    "Electronics": ["Laptop", "Smartphone", "Headphones", "Tablet", "Camera"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress"],
    "Home & Garden": ["Lamp", "Rug", "Plant Pot", "Cushion", "Mirror"],
    "Books": ["Novel", "Textbook", "Cookbook", "Biography", "Guide"],
    "Sports": ["Running Shoes", "Yoga Mat", "Dumbbells", "Bicycle", "Tennis Racket"],
    "Beauty": ["Moisturizer", "Shampoo", "Perfume", "Lipstick", "Sunscreen"],
    "Toys": ["Board Game", "Puzzle", "Action Figure", "LEGO Set", "Doll"],
    "Food & Beverage": ["Coffee", "Tea Set", "Chocolate Box", "Wine", "Snack Pack"],
}
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "bank_transfer"]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
EVENT_TYPES = ["page_view", "click", "add_to_cart", "purchase", "search"]
PAGE_URLS = [
    "/home", "/products", "/products/electronics", "/products/clothing",
    "/cart", "/checkout", "/account", "/search", "/about", "/contact",
    "/products/detail/123", "/products/detail/456", "/promotions",
]
TICKET_CATEGORIES = ["billing", "shipping", "product_quality", "account",
                     "technical", "returns", "general"]
PRIORITIES = ["low", "medium", "high", "critical"]
STATUSES = ["open", "in_progress", "resolved", "closed"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customer Data (JSON)

# COMMAND ----------

def generate_customers():
    """Generate realistic customer JSON data."""

    customers = []
    base_date = datetime(2020, 1, 1)

    for i in range(NUM_CUSTOMERS):
        customer_id = f"C-{1000 + i}"
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        city, state = random.choice(CITIES)
        created_date = base_date + timedelta(days=random.randint(0, 1500))
        updated_date = created_date + timedelta(days=random.randint(0, 365))

        customer = {
            "customer_id": customer_id,
            "email": f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@example.com",
            "first_name": first_name,
            "last_name": last_name,
            "phone": f"+1{random.randint(200, 999)}{random.randint(1000000, 9999999)}",
            "created_date": created_date.isoformat(),
            "updated_date": updated_date.isoformat(),
            "loyalty_tier": random.choices(LOYALTY_TIERS, weights=[40, 30, 20, 10])[0],
            "lifetime_value": round(random.uniform(0, 25000), 2),
            "address": {
                "street": f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Maple', 'Cedar'])} St",
                "city": city,
                "state": state,
                "zip_code": f"{random.randint(10000, 99999)}",
                "country": "US"
            }
        }
        customers.append(customer)

    # Write as JSON files (simulating multiple file deliveries)
    batch_size = NUM_CUSTOMERS // 5
    for batch_num in range(5):
        batch = customers[batch_num * batch_size:(batch_num + 1) * batch_size]
        df = spark.read.json(spark.sparkContext.parallelize(
            [json.dumps(c) for c in batch]
        ))
        output_path = f"{LANDING_PATH}/crm_customers/batch_{batch_num}.json"
        df.coalesce(1).write.mode("overwrite").json(
            f"{LANDING_PATH}/crm_customers/batch_{batch_num}"
        )

    print(f"Generated {NUM_CUSTOMERS} customers in 5 JSON batches")
    return customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Transaction Data (CSV)

# COMMAND ----------

def generate_transactions(customer_ids):
    """Generate realistic transaction CSV data."""

    transactions = []
    base_date = datetime(2024, 1, 1)

    for i in range(NUM_TRANSACTIONS):
        category = random.choice(PRODUCT_CATEGORIES)
        product = random.choice(PRODUCT_NAMES[category])
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(9.99, 499.99), 2)

        txn = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": random.choice(customer_ids),
            "transaction_date": (base_date + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).isoformat(),
            "amount": round(unit_price * quantity, 2),
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_pct": round(random.choices(
                [0.0, 0.05, 0.10, 0.15, 0.20, 0.25],
                weights=[50, 15, 15, 10, 5, 5]
            )[0], 2),
            "product_id": f"P-{random.randint(1000, 9999)}",
            "product_name": product,
            "product_category": category,
            "store_id": f"S-{random.randint(1, 50)}",
            "payment_method": random.choice(PAYMENT_METHODS),
            "currency": "USD",
        }
        transactions.append(txn)

    # Write as CSV files
    df = spark.createDataFrame(transactions)
    batch_size = NUM_TRANSACTIONS // 5
    for batch_num in range(5):
        batch_df = df.limit(batch_size).offset(batch_num * batch_size) if hasattr(df, 'offset') else df
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            f"{LANDING_PATH}/transactions/batch_{batch_num}"
        )

    print(f"Generated {NUM_TRANSACTIONS} transactions in CSV format")
    return transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Clickstream Data (Parquet)

# COMMAND ----------

def generate_clickstream(customer_ids):
    """Generate realistic clickstream Parquet data."""

    events = []
    base_date = datetime(2024, 1, 1)

    for i in range(NUM_CLICKSTREAM_EVENTS):
        event_time = base_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        event = (
            random.choice(customer_ids),                            # customer_id
            event_time,                                              # event_timestamp
            random.choice(EVENT_TYPES),                             # event_type
            random.choice(PAGE_URLS),                               # page_url
            random.choice(DEVICES),                                 # device_type
            random.choice(BROWSERS),                                # browser
            random.choice(["google", "direct", "email", "social", "ad"]),  # referrer
            f"session_{random.randint(1, NUM_CLICKSTREAM_EVENTS // 10)}",  # session_hint
        )
        events.append(event)

    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("event_timestamp", TimestampType()),
        StructField("event_type", StringType()),
        StructField("page_url", StringType()),
        StructField("device_type", StringType()),
        StructField("browser", StringType()),
        StructField("referrer", StringType()),
        StructField("session_hint", StringType()),
    ])

    df = spark.createDataFrame(events, schema)
    df.repartition(10).write.mode("overwrite").parquet(
        f"{LANDING_PATH}/clickstream"
    )

    print(f"Generated {NUM_CLICKSTREAM_EVENTS} clickstream events in Parquet format")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Support Ticket Data (JSON)

# COMMAND ----------

def generate_support_tickets(customer_ids):
    """Generate realistic support ticket JSON data."""

    tickets = []
    base_date = datetime(2024, 1, 1)

    for i in range(NUM_SUPPORT_TICKETS):
        created = base_date + timedelta(days=random.randint(0, 365))
        status = random.choices(STATUSES, weights=[20, 15, 35, 30])[0]
        resolved_at = None
        if status in ("resolved", "closed"):
            resolved_at = (created + timedelta(
                hours=random.randint(1, 168)
            )).isoformat()

        ticket = {
            "ticket_id": f"TKT-{1000 + i}",
            "customer_id": random.choice(customer_ids),
            "subject": f"Issue with {random.choice(['order', 'delivery', 'payment', 'account', 'product'])}",
            "category": random.choice(TICKET_CATEGORIES),
            "priority": random.choices(PRIORITIES, weights=[30, 40, 20, 10])[0],
            "status": status,
            "created_at": created.isoformat(),
            "resolved_at": resolved_at,
            "agent_id": f"A-{random.randint(100, 120)}",
            "satisfaction_score": random.randint(1, 5) if status in ("resolved", "closed") else None,
        }
        tickets.append(ticket)

    df = spark.read.json(spark.sparkContext.parallelize(
        [json.dumps(t) for t in tickets]
    ))
    df.coalesce(1).write.mode("overwrite").json(
        f"{LANDING_PATH}/support_tickets"
    )

    print(f"Generated {NUM_SUPPORT_TICKETS} support tickets in JSON format")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate All Data

# COMMAND ----------

print("=" * 60)
print("GENERATING SAMPLE DATA FOR CUSTOMER 360 PLATFORM")
print("=" * 60)

# Generate customers first (we need their IDs for other data)
customers = generate_customers()
customer_ids = [c["customer_id"] for c in customers]

# Generate dependent datasets
generate_transactions(customer_ids)
generate_clickstream(customer_ids)
generate_support_tickets(customer_ids)

print("\n" + "=" * 60)
print("DATA GENERATION COMPLETE")
print(f"  Customers: {NUM_CUSTOMERS}")
print(f"  Transactions: {NUM_TRANSACTIONS}")
print(f"  Clickstream Events: {NUM_CLICKSTREAM_EVENTS}")
print(f"  Support Tickets: {NUM_SUPPORT_TICKETS}")
print(f"  Landing Path: {LANDING_PATH}")
print("=" * 60)
