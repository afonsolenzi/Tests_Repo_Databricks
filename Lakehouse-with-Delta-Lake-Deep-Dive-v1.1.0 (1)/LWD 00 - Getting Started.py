# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Retrieve Raw Data
# MAGIC
# MAGIC **Objective:** In this notebook, you will ingest data from a remote source into our source directory, **`raw`**.

# COMMAND ----------

# MAGIC %md ## Classroom Setup
# MAGIC Run the following cell to configure this course's environment.
# MAGIC
# MAGIC Once it is done runnign there are several key points to draw attention to:
# MAGIC * We created a database for you - this ensures that you won't have conflicts with other users in the workspace
# MAGIC * We "install" datasets used by this course
# MAGIC * We validate that the datasets were "installed" correctly
# MAGIC * Our Classroom-Setup script defines various paths that we will use throughout the course
# MAGIC * We enumerate an tables we predefined for you

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Our Dataset
# MAGIC
# MAGIC The **`DA`** object is provided by Databricks Academy and is used to advertise functions and variables unique to this course.
# MAGIC
# MAGIC The first one of interest is **`DA.paths.raw`** which we can use to review the files "installed" to our workspace.
# MAGIC
# MAGIC To do this, we will employ a Databricks/notebook utility called **`dbutils`** to list the files in the path specified.

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.raw)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Exercise
# MAGIC Using the **`dbutils`** object and the path defined by **`DA.paths.working_dir`**, list the files in this new directory.

# COMMAND ----------

# TODO


# COMMAND ----------

# MAGIC %md Now test to make sure you have the right set of files.

# COMMAND ----------

assert len(files) == 1, f"Expected 1 file, found {len(files)}"
print("All tests passed")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
