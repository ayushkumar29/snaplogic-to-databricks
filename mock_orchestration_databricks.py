import os
from pyspark.sql import SparkSession

# Create a SparkSession (if not implicitly available)
try:
    spark = SparkSession.getActiveSparkSession()
except NameError:
    spark = SparkSession.builder.appName('Pipeline Orchestration').getOrCreate()

# Define the pipeline logic
def execute_sp_get_orders(spark):
    # Read from SQL Server Stored Procedure
    df = spark.read.format("jdbc").option("dbtable", "EXEC sp_GetOrders").option("schema", "dbo").load()
    
    return df

def check_status(df):
    # Check Status using Router Snap
    routes = [
        {"expression": "$status == 'Failed'"},  # Failed pipeline
        {"expression": "$status == 'Success'"}  # Successful pipeline
    ]
    
    status_df = df.filter([r["expression"] for r in routes])
    return status_df

def send_failure_email(df):
    # Send Failure Email using Email Snap
    to = "admin@company.com"
    subject = "Pipeline Failed"
    body_type = "HTML"
    
    try:
        # placeholder function - implement smtplib
        # TODO: Use a library like yagmail for easier email sending
        pass
    except Exception as e:
        print(f"Error sending email: {e}")

def run_downstream_process(df):
    # Run Downstream Process using PipeExec Snap
    pipeline = "projects/ETL/Process_Orders"
    timeout = 300
    
    try:
        dbutils.notebook.run(pipeline, timeout)
    except Exception as e:
        print(f"Error running downstream process: {e}")

def main():
    # Start the pipeline
    df = execute_sp_get_orders(spark)
    
    status_df = check_status(df)
    
    if "Failed" in status_df.columns:
        send_failure_email(status_df)
        
    run_downstream_process(status_df)

if __name__ == "__main__":
    main()