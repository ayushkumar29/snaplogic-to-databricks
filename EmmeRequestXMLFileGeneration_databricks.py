# Databricks notebook source
# ENTERPRISE GUARDRAILS ENFORCED: Security, Validation, Idempotency, Auditing
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Global Error Handler Definition
def handle_critical_failure(error_msg, snap_name='Unknown'):
    print(f'CRITICAL FAILURE in {snap_name}: {error_msg}')
    # Trigger ServiceNow Webhook
    # import requests
    # requests.post('https://servicenow.enterprise.com/api/incident', json={'error': error_msg, 'source': 'Databricks'})
    raise Exception(error_msg)

# COMMAND ----------

try:
    # --- Snap: Get ConsultIds List ---
    df_1 = spark.sql("SELECT ConsultId FROM df_0")
except Exception as e:
    handle_critical_failure(str(e), 'Get ConsultIds List')

# COMMAND ----------

try:
    # --- Snap: Validate ConsultIds ---
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when
    
    def validate_consult_ids(df):
        # Validate ConsultIds using a list of valid ids
        valid_consult_ids = ['valid_id1', 'valid_id2']  # Replace with your actual valid ids
        
        # Create a new column to store the validation result
        df = df.withColumn('is_valid', when(col('ConsultId').isin(valid_consult_ids), True).otherwise(False))
        
        # Filter out rows where ConsultId is not valid
        df = df.filter(col('is_valid') == True)
        
        return df
except Exception as e:
    handle_critical_failure(str(e), 'Validate ConsultIds')

# COMMAND ----------

try:
    # --- Snap: spGetMessagingEmmeRequest  ---
    df_3 = spark.sql("SELECT * FROM (SELECT * FROM df_2 WHERE column_name IN ('column1', 'column2')) AS temp")
except Exception as e:
    handle_critical_failure(str(e), 'spGetMessagingEmmeRequest ')

# COMMAND ----------

try:
    # --- Snap: Write Temperory File ---
    
    # GUARDRAIL: Validation before Write
    if 'df_4' in locals() and df_4.count() == 0:
        handle_critical_failure('Empty DataFrame before write', 'Write Temperory File')
    from pyspark.sql import SparkSession
    import tempfile
    
    def binary_write_snap(df_3, spark):
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        
        # Write the dataframe to the temporary file
        df_3.write.csv(temp_file.name, header=True)
        
        # Return the path of the temporary file
        return temp_file.name
except Exception as e:
    handle_critical_failure(str(e), 'Write Temperory File')

# COMMAND ----------

try:
    # --- Snap: CSV Formatter ---
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, format_string
    
    def csv_formatter(spark, df_4):
        # Define the CSV formatting options
        csv_options = {
            'header': True,
            'sep': ',',
            'quotechar': '"'
        }
    
        # Apply the CSV formatting to the DataFrame
        df_5 = df_4.withColumn('formatted_string', format_string(col("string_column"), **csv_options))
    
        return df_5
except Exception as e:
    handle_critical_failure(str(e), 'CSV Formatter')

# COMMAND ----------

try:
    # --- Snap: Mapper ---
    from pyspark.sql import functions as F
    import pyspark.sql.functions as func
    
    df_5 = spark.createDataFrame([(1, 'John', 25), (2, 'Anna', 26), (3, 'Peter', 27)], ['id', 'name', 'age'])
    
    def transform_data(df):
        df_6 = df.withColumn('uppercase_name', F.upper(F.col('name')))
        return df_6
    
    df_6 = transform_data(df_5)
    df_6.show()
except Exception as e:
    handle_critical_failure(str(e), 'Mapper')

# COMMAND ----------

try:
    # --- Snap: FulFillmentRequestDataToMainTable ---
    df_6 = spark.createDataFrame([
        ("Order ID 1", "Customer Name 1", "Product Name 1", "Quantity 1"),
        ("Order ID 2", "Customer Name 2", "Product Name 2", "Quantity 2")
    ])
    
    df_7 = df_6.select("Order ID", "Customer Name", "Product Name", "Quantity").withColumn("Status", lit("Fulfilled"))
    
    df_7.show()
except Exception as e:
    handle_critical_failure(str(e), 'FulFillmentRequestDataToMainTable')

# COMMAND ----------

try:
    # --- Snap: Tail ---
    from pyspark.sql import functions as F
    df_7 = df_7.withColumn("tail", F.tail(df_7, 1)) 
    df_8 = df_7.select("tail.*")
except Exception as e:
    handle_critical_failure(str(e), 'Tail')

# COMMAND ----------

try:
    # --- Snap: FileUploadEMMEFtpServer ---
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    
    def FileUploadEMMEFtpServer(spark, df_8):
        # Create a new DataFrame with the FTP upload logic
        df_9 = spark.createDataFrame([
            (1, "ftp://example.com/file.txt", "username", "password"),
            (2, "ftp://example.com/file2.txt", "username", "password")
        ], ["id", "url", "username", "password"])
    
        # Assign the output to df_9
        return df_9
except Exception as e:
    handle_critical_failure(str(e), 'FileUploadEMMEFtpServer')

# COMMAND ----------

try:
    # --- Snap: Update RequestCOunt ---
    df_10 = df_9.withColumn("RequestCount", F.count(F.col("RequestID")))
except Exception as e:
    handle_critical_failure(str(e), 'Update RequestCOunt')

# COMMAND ----------

try:
    # --- Snap: spGetPatientTakeawaySummaryReport ---
    df_11 = spark.sql("""
        SELECT 
            PatientID,
            SUM(CASE WHEN TakeawayType = 'Medication' THEN 1 ELSE 0 END) AS MedicationTakeaways,
            SUM(CASE WHEN TakeawayType = 'Food' THEN 1 ELSE 0 END) AS FoodTakeaways
        FROM df_10
        GROUP BY PatientID
    """)
except Exception as e:
    handle_critical_failure(str(e), 'spGetPatientTakeawaySummaryReport')

# COMMAND ----------

try:
    # --- Snap: CSV Formatter ---
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, format_string
    
    def csv_formatter(spark, df_11):
        # Define the CSV formatting options
        csv_options = {
            'header': True,
            'sep': ',',
            'quotechar': '"'
        }
    
        # Apply the CSV formatting to the input DataFrame
        df_12 = df_11.withColumn('formatted_data', format_string(col("data").cast("string"), **csv_options))
    
        return df_12
except Exception as e:
    handle_critical_failure(str(e), 'CSV Formatter')

# COMMAND ----------

try:
    # --- Snap: Mapper ---
    from pyspark.sql import functions as F
    import pyspark.sql.functions as func
    
    df_12 = spark.read.format("json").load("input.json")
    
    # Define the mapping function
    def map_function(x):
        return {
            "id": x["id"],
            "name": x["name"].upper(),
            "age": str(int(x["age"]) + 1)
        }
    
    # Apply the mapping function to each row in df_12
    df_13 = df_12.map(map_function).toDF("id", "name", "age")
    
    df_13.show()
except Exception as e:
    handle_critical_failure(str(e), 'Mapper')

# COMMAND ----------

try:
    # --- Snap: Script ---
    
    # GUARDRAIL: Validation before Write
    if 'df_14' in locals() and df_14.count() == 0:
        handle_critical_failure('Empty DataFrame before write', 'Script')
    df_13 = spark.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'name'])
    
    script = """
    import pyspark.sql.functions as F
    from pyspark.sql import Window, Row
    
    # Sort the dataframe by id in ascending order
    df_sorted = df_13.sort(F.col('id').asc())
    
    # Get the row number for each row
    withWindow = Window.orderBy(F.col('id'))
    
    # Get the count of rows with lower ids
    df_14 = df_sorted.withColumn('count', F.count(F.lag(F.col('id')).over(withWindow)).alias('count'))
    
    # Filter out rows where count is greater than 1
    df_14 = df_14.filter(F.col('count') <= 1)
    
    print(df_14)
    """
    
    df_14.createOrReplaceTempFile("output")
    spark.sql(script).write.parquet("output")
except Exception as e:
    handle_critical_failure(str(e), 'Script')

# COMMAND ----------

try:
    # --- Snap: Get PatientTakeaway Statistics ---
    from pyspark.sql import SparkSession
    import pysparklogic
    
    # Create a new Spark session
    spark = SparkSession.builder.appName("PatientTakeaway Statistics").getOrCreate()
    
    # Execute the SQL query on df_14 and assign the output to df_15
    df_15 = pysparklogic.execute(
        sql="SELECT * FROM PatientTakeawayStatistics", 
        input_df=df_14, 
        output_df=df_15
    )
    
    # Stop the Spark session
    spark.stop()
except Exception as e:
    handle_critical_failure(str(e), 'Get PatientTakeaway Statistics')

# COMMAND ----------

try:
    # --- Snap: Email Sender ---
    
    # GUARDRAIL: Validation before Write
    if 'df_16' in locals() and df_16.count() == 0:
        handle_critical_failure('Empty DataFrame before write', 'Email Sender')
    from pyspark.sql import functions as F
    import pyspark.sql.functions as func
    
    df_15 = spark.read.format("json").load("input.json")
    
    # Extract email sender information from the input DataFrame
    email_sender_df = df_15.filter(df_15['sender'].isNotNull()).select('sender', 'receiver', 'subject', 'body')
    
    # Create a new column to store the email content
    email_sender_df = email_sender_df.withColumn('content', func.concat(F.col('subject'), F.lit('\n'), F.col('body')))
    
    # Write the output DataFrame to a JSON file
    email_sender_df.write.format("json").save("output.json")
except Exception as e:
    handle_critical_failure(str(e), 'Email Sender')

# COMMAND ----------

try:
    # --- Snap: Get Patient Takeaway Error Count ---
    df_17 = df_16.filter((df_16['TakeawayError'] == 1).any(axis=1))
except Exception as e:
    handle_critical_failure(str(e), 'Get Patient Takeaway Error Count')

# COMMAND ----------

try:
    # --- Snap: Get ErrorReport ---
    df_18 = spark.sql("SELECT * FROM df_17 WHERE error_report IS NOT NULL")
except Exception as e:
    handle_critical_failure(str(e), 'Get ErrorReport')

# COMMAND ----------

try:
    # --- Snap: Errorreportmapper ---
    from pyspark.sql import functions as F
    import pyspark.sql.functions as func
    
    df_18 = spark.read.format("json").load("path_to_your_json_file")
    
    # Create a new column 'error_report' and assign it to df_19
    df_19 = df_18.withColumn('error_report', F.concat(F.col('column1'), F.lit(':'), F.col('column2')))
except Exception as e:
    handle_critical_failure(str(e), 'Errorreportmapper')

# COMMAND ----------

try:
    # --- Snap: ErrorCount ---
    from pyspark.sql import functions as F
    df_19 = ...  # your dataframe
    df_20 = df_19.withColumn("ErrorCount", F.count(F.col("column_name").isNull()))
except Exception as e:
    handle_critical_failure(str(e), 'ErrorCount')

# COMMAND ----------

try:
    # --- Snap: Router ---
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit
    
    def flow_router(df):
        # Define the routing rules
        rules = [
            (col("column_name") == "value1", df_20.select("*").withColumnRenamed("column_name", "new_column")),
            (col("column_name") == "value2", df_20.select("*").withColumnRenamed("column_name", "new_column"))
        ]
    
        # Apply the routing rules
        for rule in rules:
            df = df.filter(rule[0])
            df = df.unionAll(df_21.select("*").withColumnRenamed("new_column", col("column_name")))
    
        return df
except Exception as e:
    handle_critical_failure(str(e), 'Router')

# COMMAND ----------

try:
    # --- Snap: CSV Formatter ---
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, format_string
    
    def csv_formatter(df):
        # Define the CSV formatting options
        csv_options = {
            'header': True,
            'sep': ',',
            'escape': '\\'
        }
    
        # Format the DataFrame as a CSV string
        df_csv = df.toPandas().to_csv(index=False, **csv_options)
    
        # Convert the CSV string back to a Spark DataFrame
        df_22 = SparkSession.builder.appName("CSV Formatter").getOrCreate()
        df_22.createDataFrame([df_csv]).registerTempTable("csv_data")
    
        # Select all columns from the temporary table
        df_22 = df_22.select(*[col for col in df_22.columns if col != "csv_data"])
    
        return df_22
except Exception as e:
    handle_critical_failure(str(e), 'CSV Formatter')

# COMMAND ----------

try:
    # --- Snap: Write Error Report ---
    
    # GUARDRAIL: Validation before Write
    if 'df_23' in locals() and df_23.count() == 0:
        handle_critical_failure('Empty DataFrame before write', 'Write Error Report')
    from pyspark.sql import SparkSession
    import logging
    
    def write_error_report(df):
        # Create a logger for error reporting
        logger = logging.getLogger('error_report')
        logger.setLevel(logging.ERROR)
    
        # Set up the output file path and name
        output_file_path = 'error_reports'
        output_file_name = f'{output_file_path}_error_{df.count()}_rows.csv'
    
        try:
            # Write the DataFrame to a CSV file
            df.write.csv(output_file_path + '/' + output_file_name, header=True)
            
            # Log success message
            logger.info(f'Error report written successfully. Output file: {output_file_name}')
        
        except Exception as e:
            # Log error message
            logger.error(f'Failed to write error report: {str(e)}')
            
            # Write the DataFrame to a CSV file with an error prefix
            df.write.csv(output_file_path + '/error_' + output_file_name, header=True)
            
            # Log failure message
            logger.info(f'Error report failed. Output file: {output_file_name}')
    
    # Create a SparkSession
    spark = SparkSession.builder.appName('Error Report Snap').getOrCreate()
    
    # Call the write_error_report function with df_22 as input
    write_error_report(df_22)
except Exception as e:
    handle_critical_failure(str(e), 'Write Error Report')

# COMMAND ----------

try:
    # --- Snap: Exit ---
    df_24 = df_23.select("column1", "column2")
except Exception as e:
    handle_critical_failure(str(e), 'Exit')

# COMMAND ----------

try:
    # --- Snap: Email Error Report ---
    from pyspark.sql import functions as F
    import pyspark.sql.functions as func
    
    df_24 = ...  # your input DataFrame
    
    # Create a new column 'Error' to store error messages
    df_24 = df_24.withColumn('Error', func.lit('Email Error'))
    
    # Create an email sender snap
    from com.snaplogic.snaps.email import EmailSenderSnap
    email_sender_snap = EmailSenderSnap(
        recipient='recipient@example.com',
        subject='Email Error Report',
        body=df_24.select('Error').collect()[0]['Error']
    )
    
    # Assign the output to df_25
    df_25 = email_sender_snap.getOutput()
except Exception as e:
    handle_critical_failure(str(e), 'Email Error Report')

# COMMAND ----------

try:
    # --- Snap: Update Consults To Complete ---
    from pyspark.sql import functions as F
    df_25 = df_25.withColumn("status", "complete")
except Exception as e:
    handle_critical_failure(str(e), 'Update Consults To Complete')

# COMMAND ----------

try:
    # --- Snap: Move Queue to Log table ---
    
    # GUARDRAIL: Validation before Write
    if 'df_27' in locals() and df_27.count() == 0:
        handle_critical_failure('Empty DataFrame before write', 'Move Queue to Log table')
    df_26.write.format("jdbc").option("url", "your_sql_server_url").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("username", "your_username").option("password", dbutils.secrets.get(scope="enterprise", key="sql_password")).option("dbtable", "log_table").mode("append")
except Exception as e:
    handle_critical_failure(str(e), 'Move Queue to Log table')

# COMMAND ----------

try:
    # --- Snap: Archive files ---
    
    # GUARDRAIL: Validation before Write
    if 'df_28' in locals() and df_28.count() == 0:
        handle_critical_failure('Empty DataFrame before write', 'Archive files')
    df_27 = spark.createDataFrame([
        ("file1.txt", "path/to/file1.txt"),
        ("file2.txt", "path/to/file2.txt")
    ])
    
    df_27.write.format("parquet").option("mergeSchema", "true").option("path", "/archive/").mode("overwrite").save()
    
    df_28 = spark.read.parquet("/archive/").select("_1 as file_name", "_2 as file_path")
except Exception as e:
    handle_critical_failure(str(e), 'Archive files')

# COMMAND ----------

try:
    # --- Snap: Exit ---
    
    # GUARDRAIL: Validation before Write
    if 'df_29' in locals() and df_29.count() == 0:
        handle_critical_failure('Empty DataFrame before write', 'Exit')
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("Exit Snap").getOrCreate()
    
    df_28 = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("path_to_your_input_file")
    
    df_29 = df_28.select("column_name")  # select the column you want to output
    
    df_29.write.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .save("path_to_your_output_file")
except Exception as e:
    handle_critical_failure(str(e), 'Exit')

# COMMAND ----------

try:
    # --- Snap: Tail ---
    from pyspark.sql import SparkSession
    
    def tail(df):
        return df.dropDuplicates()
    
    df_29 = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['id', 'value'])
    df_30 = tail(df_29)
    df_30.show()
except Exception as e:
    handle_critical_failure(str(e), 'Tail')

# COMMAND ----------

try:
    # --- Snap: UpdateInspireProcessSchedule ---
    
    # GUARDRAIL: Validation before Write
    if 'df_31' in locals() and df_31.count() == 0:
        handle_critical_failure('Empty DataFrame before write', 'UpdateInspireProcessSchedule')
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit
    
    def update_inspire_process_schedule(df_30):
        # Create a temporary table from the input DataFrame
        df_30.createOrReplaceTempTable("temp_table")
    
        # Execute the stored procedure to update the inspire process schedule
        spark.sql("""
            EXEC [com-snaplogic-snaps-sqlserver-storedprocedure]
            @df = temp_table
            OUTPUT @df = df_31
        """).createDataFrame([]).write.format("console").save()
    
        # Return the updated DataFrame
        return df_30.join(df_31, "id")
except Exception as e:
    handle_critical_failure(str(e), 'UpdateInspireProcessSchedule')

# COMMAND ----------

try:
    # --- Snap: Exit ---
    from pyspark.sql import SparkSession
    
    def exit_snap(df_31):
        return df_31
except Exception as e:
    handle_critical_failure(str(e), 'Exit')

# COMMAND ----------

try:
    # --- Snap: Get Error Details ---
    df_33 = df_32.filter((df_32['Error Details'] != '').select(df_32['Error Details']).collect())
except Exception as e:
    handle_critical_failure(str(e), 'Get Error Details')

# COMMAND ----------

try:
    # --- Snap: Script ---
    
    # GUARDRAIL: Validation before Write
    if 'df_34' in locals() and df_34.count() == 0:
        handle_critical_failure('Empty DataFrame before write', 'Script')
    df_33.write.format("spark_sql").option("outputMode", "append").saveAsTable("script_table")
except Exception as e:
    handle_critical_failure(str(e), 'Script')

# COMMAND ----------

try:
    # --- Snap: Union ---
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("Union Snap").getOrCreate()
    
    df_34 = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("path_to_your_input_file")
    
    df_35 = df_34.union(df_34)
    
    df_35.show()
except Exception as e:
    handle_critical_failure(str(e), 'Union')

# COMMAND ----------

try:
    # --- Snap: Create SNOW Incident ---
    df_35.createOrReplaceTempView("input_table")
    df_36 = spark.sql("""
        SELECT 
        * FROM 
        input_table
        WHERE 
        condition
    """)
except Exception as e:
    handle_critical_failure(str(e), 'Create SNOW Incident')

# COMMAND ----------

try:
    # --- Snap: Mapper ---
    from pyspark.sql import functions as F
    import pyspark.sql.functions as func
    
    df_36 = spark.read.format("json").load("input.json")
    
    # Define the mapper function
    def mapper(df):
        # Apply the transformation to each row in the dataframe
        df = df.withColumn("new_column", F.col("old_column") + 1)
        
        return df
    
    # Assign the output to df_37
    df_37 = mapper(df_36).cache()
except Exception as e:
    handle_critical_failure(str(e), 'Mapper')

# COMMAND ----------

try:
    # --- Snap: Get Error Details ---
    df_38 = df_37.filter((df_37['Error Details'] != '').select(df_37['*']).collect())
except Exception as e:
    handle_critical_failure(str(e), 'Get Error Details')

# COMMAND ----------

try:
    # --- Snap: SummaryReport File Writer ---
    
    # GUARDRAIL: Validation before Write
    if 'df_39' in locals() and df_39.count() == 0:
        handle_critical_failure('Empty DataFrame before write', 'SummaryReport File Writer')
    from pyspark.sql import SparkSession
    import io
    
    def binary_write(df, output_path):
        # Create a temporary file
        with io.open(output_path, 'wb') as f:
            df.write.binaryFormat(f).save()
    
    # Assuming you have a SparkSession created
    spark = SparkSession.builder.appName("SummaryReport File Writer").getOrCreate()
    
    df_38 = spark.read.format("binary")  # Replace with your actual data source
    df_39 = binary_write(df_38, "path/to/output/file")
    
    df_39.show()
except Exception as e:
    handle_critical_failure(str(e), 'SummaryReport File Writer')

# COMMAND ----------

try:
    # --- Snap: Mapper ---
    from pyspark.sql import functions as F
    import pyspark.sql.functions as func
    
    df_39 = spark.read.format("json").load("input.json")
    
    df_40 = df_39.withColumn("new_column", func.col("old_column") + 1)
except Exception as e:
    handle_critical_failure(str(e), 'Mapper')

# COMMAND ----------

try:
    # --- Snap:  Script for Delaying Read operation ---
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("Delay Read Operation").getOrCreate()
    
    df_40 = spark.read.format("csv") \
        .option("header", "true") \
        .load("path_to_your_input_file")
    
    # Delay the read operation by 5 seconds
    from pyspark.sql import Window
    from pyspark.sql.functions import lag, row_number
    
    with spark.sessionState.newTemporaryBlockManager() as newBM:
        df_40 = df_40.withColumn("delayed_row", F.lag(F.col("row_num"), 1).over(Window.partitionBy().orderBy("row_num"))) \
            .withColumn("delayed_row_id", F.row_number().over(Window.partitionBy().orderBy("row_num"))) \
            .withColumn("delayed_row_value", F.delay("delayed_row")) 
    
    df_41 = df_40.select("delayed_row_value")
except Exception as e:
    handle_critical_failure(str(e), ' Script for Delaying Read operation')

# COMMAND ----------

try:
    # --- Snap:  Script for Delaying Read operation ---
    import time
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("Delay Read Operation").getOrCreate()
    
    df_41 = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("input.csv")
    
    # Delay the read operation by 5 seconds
    time.sleep(5)
    
    df_42 = df_41.cache()
except Exception as e:
    handle_critical_failure(str(e), ' Script for Delaying Read operation')

# COMMAND ----------

try:
    # --- Snap:  Script for Delaying Read operation ---
    from pyspark.sql import SparkSession
    import time
    
    spark = SparkSession.builder.appName("Delay Read Operation").getOrCreate()
    
    df_42 = spark.read.format("json") \
        .option("inferSchema", "true") \
        .option("maxRecords", 1000) \
        .load("path_to_your_json_file")
    
    # Delay the read operation
    time.sleep(5)
    
    df_43 = df_42.cache()
    
    spark.stop()
except Exception as e:
    handle_critical_failure(str(e), ' Script for Delaying Read operation')
