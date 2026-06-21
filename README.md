# Shauryansh_week_5_assignment
#  Week 5 — Spark Fundamentals & Data Cleaning  and Core Transformations

Welcome to the **Spark Fundamentals, Data Cleaning, and Core Transformations** repository. This project contains conceptual explanations and production-ready PySpark pipeline queries developed during the Celebal Summer Internship. The codebase demonstrates core big data processing strategies, data quality workflows, and aggregation optimization techniques.

---

## 📌 Intern Details
* **Name:** Shauryansh Chauhan 
* **Institution:** MMDU 
* **Course:** B.Tech Computer Science & Engineering
  
## 📌 platform used : databricks 

---

## 🚀 Key Learning Objectives
* Overcoming traditional **Hadoop MapReduce limitations** via memory-optimized infrastructure.
* Implementing robust **data cleaning filters** (Handling Null inputs, structural deduplication, and empty spaces).
* Utilizing **Narrow vs. Wide transformations** efficiently within distributed node clusters.
* Architecting decoupled end-to-end data processing pipelines using data functions.

---

## 🛠️ Environment Configuration & Mock Data Setup
To execute the processing queries, the following mock dataset is loaded into the active memory layer. The dataset intentionally includes structural anomalies (duplicates, missing elements, and incorrectly formatted parameters) to test our parsing logic:

# ---------------------------------------------------------
# 1. INITIALIZE SPARK SESSION
# ---------------------------------------------------------
# If running in Databricks, the 'spark' session is already active.
# This block ensures compatibility outside Databricks as well.
spark = SparkSession.builder \
    .appName("Celebal_Week_5_Assignment") \
    .getOrCreate()

# ---------------------------------------------------------
# 2. DATA SETUP: SOURCE MOCK DATASET
# ---------------------------------------------------------
# Generating a dataset containing deliberate anomalies (duplicates, nulls, empty strings)
raw_transactions_data = [
    ("user_1", "2026-06-20", "West", "Electronics", 500.0, 25, "New York", "user1@email.com", "user_one", "2026-06-20 10:00:00", "Store_A"),
    ("user_1", "2026-06-20", "West", "Electronics", 500.0, 25, "New York", "user1@email.com", "user_one", "2026-06-20 10:00:00", "Store_A"), # Duplicate row
    ("user_2", "2026-06-21", "East", "Electronics", None, 40, "Chicago", "user2@email.com", "user_two", "2026-06-21 11:00:00", "Store_B"),    # Null price
    ("user_3", "2026-06-21", "West", "Clothing", 150.0, 22, "New York", None, "user_three", "2026-06-21 12:00:00", "Store_A"),            # Null email
    ("user_4", "2026-06-22", "North", "Clothing", 80.0, 17, "Los Angeles", "user4@email.com", "", "2026-06-22 13:00:00", "Store_C"),         # Empty username
]

schema_columns = [
    "user_id", "transaction_date", "region", "product_category", 
    "price", "age", "city", "email", "username", "raw_timestamp", "store_id"
]

# Create our base DataFrame
df = spark.createDataFrame(raw_transactions_data, schema=schema_columns)
df_sales = df # Creating an alias for regional sales tasks

print("=" * 60)
print("BASE DATASET LOADED")
print("=" * 60)
df.show()

# ---------------------------------------------------------
# Q3: DATA DEDUPLICATION
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("Q3: REMOVE DUPLICATES BASED ON COMPOSITE KEY")
print("=" * 60)

target_business_keys = ["user_id", "transaction_date"]
df_cleaned_dupes = df.dropDuplicates(subset=target_business_keys)
df_cleaned_dupes.show()

# ---------------------------------------------------------
# Q4: REGIONAL PERFORMANCE AGGREGATION
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("Q4: FILTER REGION AND CALCULATE AVERAGE PRICES")
print("=" * 60)

west_coast_sales_df = df_sales.filter(F.col("region") == "West")
category_performance_df = (
    west_coast_sales_df
    .groupBy("product_category")
    .agg(F.avg("price").alias("avg_sale_amount"))
)
category_performance_df.show()

# ---------------------------------------------------------
# Q5: NULL VALUE IMPUTATION
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("Q5: IMPUTE NULL STRINGS WITH FALLBACK LABELS")
print("=" * 60)

default_status_fallback = {"username": "Unknown"}
df_with_imputed_status = df.na.fill(default_status_fallback)
df_with_imputed_status.show()

# ---------------------------------------------------------
# Q6: HIGH-VOLUME CITY GROUPING (THRESHOLD METRICS)
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("Q6: TOTAL COUNT PER CITY (WITH MINIMUM THRESHOLD)")
print("=" * 60)

# Note: Using limit > 0 for demo purposes since mock data has few records
city_metrics_df = (
    df.groupBy("city")
    .count()
    .filter(F.col("count") > 0) # Adjust threshold to 100 for production data
)
city_metrics_df.show()

# ---------------------------------------------------------
# Q8: TARGET DEMOGRAPHICS FILTER
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("Q8: FILTER TARGET AGE & SUBSCRIPTION DEMOGRAPHICS")
print("=" * 60)

is_target_age_group = F.col("age").between(18, 30)
is_premium_subscriber = F.col("region") == "West"

target_demographic_df = df.filter(is_target_age_group & is_premium_subscriber)
target_demographic_df.show()

# ---------------------------------------------------------
# Q10: SCHEMA MODIFICATION (CASTING & RENAMING)
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("Q10: NORMALIZE SCHEMA (CAST AND RENAME RAW FIELDS)")
print("=" * 60)

df_normalized_time = (
    df.withColumn("event_time", F.col("raw_timestamp").cast(TimestampType()))
    .drop("raw_timestamp")
)
df_normalized_time.show()

# ---------------------------------------------------------
# Q12: CONTACT VALIDATION & QUALITY ENFORCEMENT
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("Q12: VALIDATE CONTACT INFO AND DROP BROKEN ROWS")
print("=" * 60)

has_valid_email = F.col("email").isNotNull()
has_valid_username = F.col("username") != ""

df_verified_contacts = df.filter(has_valid_email & has_valid_username)
df_verified_contacts.show()

# ---------------------------------------------------------
# Q13: PRODUCT PRICING DISTRIBUTION METRICS
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("Q13: EXTRACT STATISTICAL SNAPSHOT IN A SINGLE PASS")
print("=" * 60)

pricing_distribution_df = df.agg(
    F.min("price").alias("minimum_price"),
    F.max("price").alias("maximum_price"),
    F.mean("price").alias("mean_average_price")
)
pricing_distribution_df.show()

# ---------------------------------------------------------
# Q15: MODULAR END-TO-END DATA PROCESSING PIPELINE
# ---------------------------------------------------------
print("\n" + "=" * 60)
print("Q15: RUN INTEGRATED CLEANING AND REVENUE PIPELINE")
print("=" * 60)

def clean_and_calculate_revenue(input_dataframe):
    """
    Cleans raw transaction logs and aggregates revenue metrics by distribution node.
    """
    # Step 1: Remove completely duplicated transaction logs
    cleaned_records = input_dataframe.dropDuplicates()
    
    # Step 2: Establish base valuations for missing pricing inputs to secure sums
    imputed_pricing_records = cleaned_records.na.fill({"price": 0.0})
    
    # Step 3: Aggregate commercial value across store touchpoints
    store_revenue_analytics = (
        imputed_pricing_records
        .groupBy("store_id")
        .agg(F.sum("price").alias("total_revenue"))
    )
    
    return store_revenue_analytics

# Run unified pipeline and show final aggregated outcomes
final_analysis_output = clean_and_calculate_revenue(df)
final_analysis_output.show()

schema_columns = ["user_id", "transaction_date", "region", "product_category", "price", "age", "city", "email", "username", "raw_timestamp", "store_id"]

# Load into Spark DataFrame (The session 'spark' is pre-initialized natively within Databricks)
df = spark.createDataFrame(raw_transactions_data, schema=schema_columns)
df_sales = df
