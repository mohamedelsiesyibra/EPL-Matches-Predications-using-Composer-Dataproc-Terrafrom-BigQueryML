from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from google.auth import default
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Start a new Spark Session with the required packages
spark = SparkSession.builder \
    .appName("GCS-Databricks") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with_2.12:0.21.1") \
    .getOrCreate()

# Provide GCS bucket path
bucket_path = "gs://wr-epl-predictions-project/stagging-data"

# Separate paths for each year
years = ["2020", "2021", "2022", "2023"]
dataframes = {}

for year in years:
    df_json = spark.read.option("multiline", "true").json(f"{bucket_path}/*{year}*.json")
    df_matches = df_json.select(explode("matches").alias("matches"))

    df_final = df_matches.withColumn("date", col("matches.utcDate")) \
        .withColumn("matchday", col("matches.matchday")) \
        .withColumn("winner", col("matches.score.winner")) \
        .withColumn("homeTeam", col("matches.homeTeam.name")) \
        .withColumn("awayTeam", col("matches.awayTeam.name")) \
        .drop("matches")

    dataframes[year] = df_final

# Retrieve project ID from Google Cloud default credentials
_, project_id = default()

# Create BigQuery client
client = bigquery.Client(project=project_id)

# Create dataset if it doesn't exist
dataset_id = "premier_league_data"
dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
dataset = bigquery.Dataset(dataset_ref)
try:
    client.get_dataset(dataset)
except NotFound:
    client.create_dataset(dataset)

# Define table names
table_names = ["epl_matches_historical", "epl_matches_next_season"]

for i, table_name in enumerate(table_names):
    # Check if table exists in dataset
    table_ref = dataset_ref.table(table_name)
    try:
        client.get_table(table_ref)
        write_mode = "append"
    except NotFound:
        write_mode = "overwrite"

    # Write final DataFrame to BigQuery
    if i == 0:
        df_to_write = dataframes["2020"].union(dataframes["2021"]).union(dataframes["2022"])
    else:
        df_to_write = dataframes["2023"]

    df_to_write.write.format("bigquery") \
        .option("temporaryGcsBucket", "wr-epl-predictions-project/wr-bq-temporary-bucket") \
        .option("project", project_id) \
        .option("dataset", dataset_id) \
        .option("table", table_name) \
        .mode(write_mode) \
        .save()
