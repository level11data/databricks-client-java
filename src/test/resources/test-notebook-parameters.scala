// Databricks notebook source
dbutils.widgets.text("parameter1", "Default 1", "Parameter 1")
dbutils.widgets.text("parameter2", "Default 2", "Parameter 2")

// COMMAND ----------

val param1 = dbutils.widgets.get("parameter1")

// COMMAND ----------

val param2 = dbutils.widgets.get("parameter2")

// COMMAND ----------

val result = s"This is Parameter 1: $param1, and this is Parameter 2: $param2"

// COMMAND ----------

dbutils.notebook.exit(result.toString)
