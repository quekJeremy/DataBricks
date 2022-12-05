# Databricks notebook source
from pyspark.sql.functions import size,col,when
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType,FloatType
spark = SparkSession.builder.getOrCreate()

#dbfs:/FileStore/testing/big_file.xml
df_labels = spark.read.format('xml').options(rowTag='FIELD').load('dbfs:/FileStore/testing/rev.xml')

#df_labels = spark.read.format('xml').options(rowTag='FIELD').load('abfss://fhg@fplsinut01datalakedlsa01.dfs.core.windows.net/testing/new.xml')
df_labels=df_labels.filter("_label is not null")

df2=df_labels.select("_label","_type").collect()


df_data = spark.read.format('xml').options(rowTag='R').load('dbfs:/FileStore/testing/rev.xml')

for i in range(df_labels.count()):
  if df2[i]._type =='DATETIME':
    df_data=df_data.withColumn(df2[i]._label,df_data.D[i].cast(TimestampType()) )
  elif df2[i]._type =='VARCHAR':
    df_data=df_data.withColumn(df2[i]._label,df_data.D[i] )
  else:
    df_data=df_data.withColumn(df2[i]._label,df_data.D[i].cast(FloatType()) )
df_data=df_data.drop("D")
df_data.display()
