# Databricks notebook source
# MAGIC %md
# MAGIC ## Order Items Pipeline (Bronze -> Silver -> Gold) in ONE notebook
# MAGIC - **Bronze**: append-only, only NEW / RE-UPLOADED files are ingested. Old data is never touched.
# MAGIC   Re-uploading the same file (same date) is stored as a new `file_version`.
# MAGIC - **Schema evolution**: new columns are added automatically; missing (removed) columns become NULL.
# MAGIC - **Silver / Gold**: always built from the LATEST version of each file.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# ---------------- CONFIG ----------------
catalog_name  = "ecommerce"
landing_path  = "/Volumes/ecommerce/source_data/raw/order_items/landing/order_items_2025-11-04.csv"

bronze_table  = f"{catalog_name}.bronze.brz_order_items"
silver_table  = f"{catalog_name}.silver.slv_order_items"
gold_table    = f"{catalog_name}.gold.gld_fact_order_items"

fx_rates = {  # fixed FX rates -> INR
    "INR": 1.00, "AED": 24.18, "AUD": 57.55, "CAD": 62.93,
    "GBP": 117.98, "SGD": 68.18, "USD": 88.29,
}

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1) BRONZE  (append + versioning + schema evolution)

# COMMAND ----------

def bronze_exists():
    return spark.catalog.tableExists(bronze_table)

# --- ONE-TIME MIGRATION: bronze table created by the old notebook has no version columns.
#     Add them and back-fill, so old data is kept (as version 1) and old files are not re-ingested.
if bronze_exists():
    existing_cols = [c.lower() for c in spark.table(bronze_table).columns]
    missing = [(c, t) for c, t in [("partition_dt", "STRING"), ("file_mod_sec", "BIGINT"),
                                   ("file_version", "INT")] if c not in existing_cols]
    if missing:
        spark.sql(f"ALTER TABLE {bronze_table} ADD COLUMNS (" +
                  ", ".join(f"{c} {t}" for c, t in missing) + ")")
        print("Migration: added columns", [c for c, _ in missing])

    # back-fill legacy rows (version 1, business date from dt, mod time from the file currently in landing)
    spark.sql(f"""
        UPDATE {bronze_table}
        SET partition_dt = coalesce(cast(to_date(try_to_timestamp(trim(dt), 'yyyy-MM-dd')) AS string),
                                    cast(to_date(try_to_timestamp(regexp_extract(file_name, '(\\\\d{{4}}-\\\\d{{2}}-\\\\d{{2}})', 1),
                                                                  'yyyy-MM-dd')) AS string), 'unknown'),
            file_version = coalesce(file_version, 1)
        WHERE partition_dt IS NULL OR file_version IS NULL
    """)
    legacy_files = [(x.path, x.modificationTime // 1000) for x in dbutils.fs.ls(landing_path)
                    if x.path.lower().endswith(".csv")]
    if legacy_files:
        spark.createDataFrame(legacy_files, ["file_name", "mod_sec"]).createOrReplaceTempView("_landing_mod")
        spark.sql(f"""
            MERGE INTO {bronze_table} t USING _landing_mod s
            ON t.file_name = s.file_name AND t.file_mod_sec IS NULL
            WHEN MATCHED THEN UPDATE SET t.file_mod_sec = s.mod_sec
        """)

# --- what is already ingested? (file_name, file_mod_ts_sec) and latest version per date
def dt_key(dt_col="dt", file_col="file_name"):
    """Business-date key (yyyy-MM-dd) of a row. Taken from the data's `dt` column; if that is
    missing/invalid, falls back to a date found in the file name. Versions are tracked PER DATE,
    so a different file name carrying the same date is stored as a new version of that date."""
    from_dt   = F.to_date(F.try_to_timestamp(F.trim(F.col(dt_col)), F.lit("yyyy-MM-dd"))).cast("string")
    from_name = F.to_date(F.try_to_timestamp(
        F.regexp_extract(F.col(file_col), r"(\d{4}-\d{2}-\d{2})", 1), F.lit("yyyy-MM-dd"))).cast("string")
    return F.coalesce(from_dt, from_name, F.lit("unknown"))

ingested_keys = set()
latest_version = {}      # {business date -> latest file_version}
if bronze_exists():
    b = spark.table(bronze_table)
    if "file_mod_sec" in b.columns:
        ingested_keys = {(r["file_name"], r["file_mod_sec"])
                         for r in b.select("file_name", "file_mod_sec").distinct().collect()}
    if "file_version" in b.columns:
        key = F.coalesce(F.col("partition_dt"), dt_key()) if "partition_dt" in b.columns else dt_key()
        latest_version = {r["k"]: (r["v"] or 0)
                          for r in b.groupBy(key.alias("k")).agg(F.max("file_version").alias("v")).collect()}

# --- list landing files; a file is "new" if path OR modification time is unseen
#     (re-uploading the same file name overwrites it -> new modification time -> new version)
files = [f for f in dbutils.fs.ls(landing_path) if f.path.lower().endswith(".csv")]
new_files = sorted(
    [f for f in files if (f.path, f.modificationTime // 1000) not in ingested_keys],
    key=lambda f: f.modificationTime
)
print(f"Landing files: {len(files)} | New / re-uploaded files to ingest: {len(new_files)}")

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType

def clean_cols(cols):
    """trim + lower header names and make duplicates unique (a, a_1, a_2 ...)."""
    seen, out = {}, []
    for c in cols:
        c = (c or "").strip().lower() or "unnamed"
        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        out.append(c)
    return out

def add_meta(df, f):
    """Adds file metadata + a version PER BUSINESS DATE (not per file name).
    Returns (dataframe, {date: version}) so the version map is committed only after a successful write."""
    if "dt" not in df.columns:
        df = df.withColumn("dt", F.lit(None).cast("string"))
    df = (df.withColumn("file_name", F.lit(f.path))
            .withColumn("file_mod_sec", F.lit(f.modificationTime // 1000))
            .withColumn("partition_dt", dt_key()))
    dates = [r[0] for r in df.select("partition_dt").distinct().collect()]
    ver = {d: latest_version.get(d, 0) + 1 for d in dates}
    mapping = F.create_map(*[x for d, v in ver.items() for x in (F.lit(d), F.lit(v))])
    df = (df.withColumn("file_version", mapping[F.col("partition_dt")].cast("int"))
            .withColumn("ingest_timestamp", F.current_timestamp()))
    return df, ver

def append_bronze(df):
    (df.write.format("delta")
        .mode("append")                    # never overwrite -> previous data is untouched
        .option("mergeSchema", "true")     # schema evolution (added columns)
        .saveAsTable(bronze_table))

def read_with_spark(f):
    df = (spark.read.option("header", "true").option("delimiter", ",")
          .option("inferSchema", "false")          # everything as string in bronze
          .option("multiLine", "true")
          .csv(f.path))
    return df.toDF(*clean_cols(df.columns))

def read_with_pandas(f):
    """Fallback for files Spark cannot read (bad encoding, odd quotes, ...)."""
    local_path = f.path.replace("dbfs:", "", 1)
    pdf = pd.read_csv(local_path, dtype=str, encoding_errors="replace",
                      on_bad_lines="skip", keep_default_na=False, na_values=[""])
    pdf.columns = clean_cols(list(pdf.columns))
    schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])
    rows = [tuple(None if pd.isna(v) else v for v in r) for r in pdf.itertuples(index=False, name=None)]
    return spark.createDataFrame(rows, schema)

def ingest_file(f):
    last_err = None
    for reader in (read_with_spark, read_with_pandas):
        try:
            raw = reader(f)
            if len(raw.columns) == 0:     # nothing in the file
                print(f"  SKIP (empty file): {f.path}")
                return True
            df, ver = add_meta(raw, f)
            append_bronze(df)             # read errors surface here (lazy) -> caught below
            latest_version.update(ver)    # commit versions only after a successful write
            print(f"  OK  via {reader.__name__}: {f.path} -> versions {ver}")
            return True
        except Exception as e:
            last_err = e
            print(f"  {reader.__name__} failed for {f.path}: {str(e)[:600]}")
    failed_files.append((f.path, str(last_err)[:2000]))
    return False

failed_files = []
if new_files:
    for f in new_files:               # one file at a time, so one bad file never blocks the others
        ingest_file(f)
    print(f"Bronze done. Failed files: {len(failed_files)}")
    for p, err in failed_files:
        print("FAILED:", p, "\n   ", err)
else:
    print("Bronze: nothing new to ingest")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2) SILVER  (latest version only, cleaned)

# COMMAND ----------

bronze_df = spark.table(bronze_table)

# make sure the core columns exist even if a file dropped them
core_cols = ["dt", "order_ts", "customer_id", "order_id", "item_seq", "product_id", "quantity",
             "unit_price_currency", "unit_price", "discount_pct", "tax_amount", "channel", "coupon_code"]
for c in core_cols:
    if c not in bronze_df.columns:
        bronze_df = bronze_df.withColumn(c, F.lit(None).cast("string"))

# legacy rows (loaded before versioning existed) -> version 1
df = bronze_df.withColumn("file_version", F.coalesce(F.col("file_version"), F.lit(1)))

# keep ONLY the latest version of every business DATE (whatever the file name was).
# A newer upload for the same date -- same or different file name -- replaces the older one.
if "partition_dt" not in df.columns:
    df = df.withColumn("partition_dt", F.lit(None).cast("string"))
df = df.withColumn("partition_dt", F.coalesce(F.col("partition_dt"), dt_key()))
w_file = Window.partitionBy("partition_dt")
df = (df.withColumn("_max_v", F.max("file_version").over(w_file))
        .filter(F.col("file_version") == F.col("_max_v"))
        .drop("_max_v"))

# if the same (order_id, item_seq) still appears more than once, keep the most recent load
w_key = Window.partitionBy("order_id", "item_seq").orderBy(F.col("file_version").desc(),
                                                         F.col("ingest_timestamp").desc())
df = (df.withColumn("_rn", F.row_number().over(w_key))
        .filter("_rn = 1").drop("_rn"))

# ---- cleaning ----
def num(col):  # strip everything except digits, dot, minus
    return F.regexp_replace(F.trim(F.col(col)), r"[^0-9.\-]", "").cast("double")

qty_words = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
             "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10}
quantity_clean = F.col("quantity")
for word, n in qty_words.items():
    quantity_clean = F.when(F.lower(F.trim(F.col("quantity"))) == word, F.lit(str(n))).otherwise(quantity_clean)

df = (df
      .withColumn("quantity", quantity_clean.cast("int"))
      .withColumn("unit_price", num("unit_price"))
      .withColumn("discount_pct", num("discount_pct"))
      .withColumn("tax_amount", num("tax_amount"))
      .withColumn("coupon_code",
                  F.when(F.lower(F.trim(F.col("coupon_code"))).isin("", "null", "none", "nan"), F.lit(None))
                   .otherwise(F.lower(F.trim(F.col("coupon_code")))))
      .withColumn("channel",
                  F.when(F.lower(F.col("channel")) == "web", "Website")
                   .when(F.lower(F.col("channel")) == "app", "Mobile")
                   .otherwise(F.col("channel")))
      .withColumn("dt", F.to_date("dt", "yyyy-MM-dd"))
      .withColumn("order_ts", F.coalesce(F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),
                                         F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")))
      .withColumn("item_seq", F.col("item_seq").cast("int"))
      .withColumn("unit_price_currency", F.upper(F.trim(F.col("unit_price_currency"))))
      .withColumn("processed_time", F.current_timestamp()))

(df.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")   # silver is rebuilt from latest bronze versions, new columns flow through
   .saveAsTable(silver_table))
print("Silver rebuilt:", spark.table(silver_table).count(), "rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3) GOLD  (BI-ready, latest data only)

# COMMAND ----------

sdf = spark.table(silver_table)

sdf = (sdf
       .withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
       .withColumn("discount_amount", F.ceil(F.col("gross_amount") * (F.col("discount_pct") / 100.0)))
       .withColumn("sale_amount", F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))
       .withColumn("date_id", F.date_format("dt", "yyyyMMdd").cast(IntegerType()))
       .withColumn("coupon_flag", F.when(F.col("coupon_code").isNotNull(), 1).otherwise(0)))

rates_df = spark.createDataFrame([(k, float(v)) for k, v in fx_rates.items()], ["currency", "inr_rate"])
sdf = (sdf.join(F.broadcast(rates_df), sdf.unit_price_currency == rates_df.currency, "left")
          .withColumn("sale_amount_inr", F.ceil(F.col("sale_amount") * F.col("inr_rate"))))

gold_main = [
    F.col("date_id"), F.col("dt").alias("transaction_date"), F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"), F.col("customer_id"), F.col("item_seq").alias("seq_no"),
    F.col("product_id"), F.col("channel"), F.col("coupon_code"), F.col("coupon_flag"),
    F.col("unit_price_currency"), F.col("quantity"), F.col("unit_price"), F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"), F.col("discount_amount"), F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount"), F.col("sale_amount_inr").alias("net_amount_inr"),
    F.col("file_name"), F.col("file_version"),
]

# any NEW column that arrived in the source is passed through to gold automatically
used = set(core_cols) | {"partition_dt", "file_name", "file_mod_sec", "file_version", "ingest_timestamp", "processed_time",
                         "gross_amount", "discount_amount", "sale_amount", "date_id", "coupon_flag",
                         "currency", "inr_rate", "sale_amount_inr"}
extra_cols = [F.col(c) for c in sdf.columns if c not in used]

gold_df = sdf.select(*gold_main, *extra_cols)

(gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table))

print("Gold rows:", spark.table(gold_table).count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### Sanity checks

# COMMAND ----------

display(spark.sql(f"""
  SELECT partition_dt, file_version, file_name, count(*) AS rows, max(ingest_timestamp) AS loaded_at
  FROM {bronze_table} GROUP BY partition_dt, file_version, file_name ORDER BY partition_dt, file_version
"""))