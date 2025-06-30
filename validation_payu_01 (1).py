from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col, md5, concat_ws, lit
from pyspark.sql.types import StringType
import concurrent.futures
import hashlib
 
# Your existing dataframes dictionaries
# dataframes = {}  # Your S3 dataframes dictionary (already populated)
# databricks_dataframes = {}  # Your Unity Catalog dataframes dictionary (already populated)
 
# If you haven't created these yet, here's how to load them:
 
# 1. Load S3 dataframes (as you did before)
s3_base_path = "s3://data-bricks-lake/redshift-exports/data-dump/treasury/treasury"
s3_table_names = ['adjustmentitem', 'dailymerchantsettlement', 'gstdetails',
                 'merchantconfiguration', 'paymentgateway', 'payubizadditionaldata',
                 'payubiztxn', 'payubiztxnupdate', 'saleitem']
 
dataframes = {}
for table_name in s3_table_names:
    print(f"Loading S3 table: {table_name}")
    try:
        dataframes[table_name] = spark.read.parquet(f"{s3_base_path}/{table_name}")
    except Exception as e:
        print(f"Error loading S3 table {table_name}: {e}")
 
# 2. Load Unity Catalog dataframes
catalog_name = "databrick_payuds"
schema_name = "treasury"
 
uc_table_names = ['adjustmentitem', 'dailymerchantsettlement', 'gstdetails',
                 'merchantconfiguration', 'paymentgateway', 'payubizadditionaldata',
                 'payubiztxn', 'payubiztxnupdate', 'saleitem']
 
databricks_dataframes = {}
for table_name in uc_table_names:
    print(f"Loading UC table: {table_name}")
    try:
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        databricks_dataframes[table_name] = spark.read.table(full_table_name)
    except Exception as e:
        print(f"Error loading UC table {table_name}: {e}")
 
# 3. Perform row count validation for all tables
validation_results = []
 
for table_name in s3_table_names:
    print(f"\nValidating table: {table_name}")
   
    try:
        print(f'Row count validation started for {table_name}')
        # Get S3 row count
        s3_count = dataframes[table_name].count()
       
        # Get UC row count
        uc_count = databricks_dataframes[table_name].count()
       
        # Determine if counts match
        counts_match = s3_count == uc_count
 
        print(f'Row count validation ended for {table_name}')
 
        print(f"S3 Count: {s3_count} | UC Count: {uc_count} | Status: {'MATCH' if counts_match else 'MISMATCH'}")
 
        print(f'schema validation started for {table_name}')
 
        # Get schemas
        s3_schema = {field.name: field.dataType for field in dataframes[table_name].schema.fields}
        databricks_schema = {field.name: field.dataType for field in databricks_dataframes[table_name].schema.fields}
 
        # Compare schemas
        schema_match = s3_schema == databricks_schema
       
        print(f'schema validation ended for {table_name}')
 
        print(f"Schema Match: {'MATCH' if schema_match else 'MISMATCH'}")
 
 
        print(f'row hash validation started for {table_name}')
 
         
        dataframes_s3_hash = dataframes[table_name].withColumn("row_hash",md5(concat_ws("|", *[col(c).cast(StringType()) for c in dataframes[table_name].columns]))).select("row_hash")
 
        dataframes_uc_hash = databricks_dataframes[table_name].withColumn("row_hash",md5(concat_ws("|", *[col(c).cast(StringType()) for c in databricks_dataframes[table_name].columns]))).select("row_hash")
 
        s3_unique = dataframes_s3_hash.exceptAll(dataframes_uc_hash)
        uc_unique = dataframes_uc_hash.exceptAll(dataframes_s3_hash)
 
        in_s3_not_uc = s3_unique.count()
        print("Number of rows in S3 but not in UC:", in_s3_not_uc)
       
        in_uc_not_s3 = uc_unique.count()
        print("Number of rows in UC but not in S3:", in_uc_not_s3)
       
        if in_s3_not_uc > 0 or in_uc_not_s3 > 0:
            row_hash_status = "FAIL"
        else:
            row_hash_status = "PASS"
 
        print(f'row hash validation status for {table_name} is {row_hash_status}')
        print(f'row hash validation ended for {table_name}')
        print("="*50)
       
        # Store results
        validation_results.append({
            "table_name": table_name,
            "s3_row_count": s3_count,
            "uc_row_count": uc_count,
            "row_count_status": "MATCH" if counts_match else "MISMATCH",
            "s3_schema": s3_schema,
            "databricks_schema": databricks_schema,
            "schema_match_status": "MATCH" if schema_match else "MISMATCH",
            "in_s3_not_uc": in_s3_not_uc,
            "in_uc_not_s3": in_uc_not_s3,  
            "row_hash_status": row_hash_status
        })
       
    except Exception as e:
        validation_results.append({
            "table_name": table_name,
            "error": str(e)
        })
        print(f"Error validating {table_name}: {e}")
 
# 4. Display final results in a nice table
print("\nFinal Validation Results:")
print("="*80)
results_df = spark.createDataFrame(validation_results)
results_df.show(truncate=False)
 
# Optional: Save results to a table or file
# results_df.write.saveAsTable("validation_results")
# results_df.toPandas().to_csv("row_count_validation_results.csv", index=False)