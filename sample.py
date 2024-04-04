from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Create a Spark session
spark = SparkSession.builder \
    .appName("SQL Server Data Processing") \
    .getOrCreate()

# Read data from SQL Server table
jdbc_url = "jdbc:sqlserver://your_server:1433;databaseName=your_database"
properties = {
    "user": "username",
    "password": "password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
table_name = "employees"
df = spark.read.jdbc(url=jdbc_url, table=employees, properties=properties)

# Add a new constant column (e.g., bonus_percent)
df_with_bonus = df.withColumn("bonus_percent", lit(0.3))

# Remove NaNs (null values) from a specific column (e.g., salary)
df_cleaned = df_with_bonus.na.drop(subset=["salary"])

# Save the cleaned DataFrame to Azure Data Lake Storage Gen2 as a Parquet file
output_path = "abfss://<dw_storage_account_name>@<dw_file_system>.dfs.core.windows.net/<path_to_directory>/cleaned_data.parquet"
df_cleaned.write.mode("overwrite").parquet(output_path)

# Confirm the saved file
print(f"DataFrame saved to {output_path}")

# Stop the Spark session
spark.stop()