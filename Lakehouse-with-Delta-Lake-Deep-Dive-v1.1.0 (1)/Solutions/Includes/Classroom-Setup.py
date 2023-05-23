# Databricks notebook source
# MAGIC %run ./_utility-functions

# COMMAND ----------

from pyspark.sql import DataFrame

def process_health_tracker_data(self, df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, from_unixtime
    
    return (df.withColumn("time", from_unixtime("time"))
              .withColumnRenamed("device_id", "p_device_id")
              .withColumn("time", col("time").cast("timestamp"))
              .withColumn("dte", col("time").cast("date"))
              .withColumn("p_device_id", col("p_device_id").cast("integer"))
              .select("dte", "time", "heartrate", "name", "p_device_id"))
    
DBAcademyHelper.monkey_patch(process_health_tracker_data)

# COMMAND ----------

notebook = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]
assert notebook.startswith("LWD 0")
reset = notebook.startswith("LWD 00")

# COMMAND ----------

DA = DBAcademyHelper(asynchronous=False)  # Create the DA object with the specified lesson
if reset: DA.cleanup()                    # Remove the database and any assets created by the course
DA.init(create_db=True)                   # True is the default

DA.paths.raw = f"{DA.hidden.datasets}/health_tracker/raw"
DA.paths.processed = f"{DA.paths.working_dir}/processed"

DA.install_datasets()    # Install (if necissary) and validate the datasets
DA.conclude_setup()      # Conclude the setup by printing the DA object's final state

