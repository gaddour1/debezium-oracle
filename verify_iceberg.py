from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Verify-Iceberg") \
    .master("local[*]") \
    .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.biat",
        "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.biat.type", "rest") \
    .config("spark.sql.catalog.biat.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.biat.warehouse", "s3://warehouse/") \
    .config("spark.sql.catalog.biat.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.biat.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.biat.s3.path-style-access", "true") \
    .config("spark.sql.catalog.biat.s3.access-key-id", "minioadmin") \
    .config("spark.sql.catalog.biat.s3.secret-access-key", "minioadmin") \
    .config("spark.sql.catalog.biat.s3.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── 1. Lister les tables ──
print("\n=== TABLES DISPONIBLES ===")
spark.sql("SHOW TABLES IN biat.t24").show()

# ── 2. Compter les lignes ──
print("\n=== NOMBRE DE LIGNES ===")
count_s = spark.sql("SELECT COUNT(*) as total FROM biat.t24.account_simple").collect()[0]['total']
count_m = spark.sql("SELECT COUNT(*) as total FROM biat.t24.account_multiple").collect()[0]['total']
print(f"account_simple   : {count_s} lignes")
print(f"account_multiple : {count_m} lignes")

# ── 3. Aperçu des données simples ──
print("\n=== TABLE SIMPLE (5 premières lignes) ===")
spark.sql("""
    SELECT RECID, op, kafka_ts, CURRENCY, CUSTOMER 
    FROM biat.t24.account_simple 
    LIMIT 5
""").show(truncate=False)

# ── 4. Aperçu des données multiples ──
print("\n=== TABLE MULTIPLE (10 premières lignes) ===")
spark.sql("""
    SELECT * FROM biat.t24.account_multiple 
    LIMIT 10
""").show(truncate=False)

# ── 5. Historique des snapshots (time travel) ──
print("\n=== SNAPSHOTS ICEBERG (historique) ===")
spark.sql("""
    SELECT snapshot_id, 
           committed_at, 
           operation, 
           summary['added-records'] as lignes_ajoutees,
           summary['total-records'] as total_lignes
    FROM biat.t24.account_simple.snapshots
""").show(truncate=False)

# ── 6. Jointure entre les deux tables ──
print("\n=== JOINTURE SIMPLE + MULTIPLE ===")
spark.sql("""
    SELECT s.RECID, s.CURRENCY, m.CHAMP, m.VALEUR
    FROM biat.t24.account_simple s
    JOIN biat.t24.account_multiple m ON s.RECID = m.RECID
    LIMIT 10
""").show(truncate=False)

print("\nVérification terminée !")
spark.stop()

