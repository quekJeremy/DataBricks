# Databricks notebook source
from pyspark.sql.functions import size,col,when
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType,FloatType,DateType
spark = SparkSession.builder.getOrCreate()

def xmlLoader(source):
  df_labels = spark.read.format('xml').options(rowTag='FIELD').load(source)

  #df_labels = spark.read.format('xml').options(rowTag='FIELD').load('abfss://fhg@fplsinut01datalakedlsa01.dfs.core.windows.net/testing/new.xml')
  df_labels=df_labels.filter("_label is not null")

  df2=df_labels.select("_label","_type").collect()
  df_data = spark.read.format('xml').options(rowTag='R').load(source)
  if df_data.count()>0:
    for i in range(df_labels.count()):
      if df2[i]._type =='DATETIME':
        df_data=df_data.withColumn(df2[i]._label,df_data.D[i].cast(TimestampType()) )
      if df2[i]._type =='DATE':
        df_data=df_data.withColumn(df2[i]._label,df_data.D[i].cast(DateType()) )
      elif df2[i]._type =='NUMBER':
        df_data=df_data.withColumn(df2[i]._label,df_data.D[i].cast(FloatType()) )
      elif df2[i]._type =='DECIMAL':
        df_data=df_data.withColumn(df2[i]._label,df_data.D[i].cast(FloatType()) )
      else:
        df_data=df_data.withColumn(df2[i]._label,df_data.D[i] )
    df_data=df_data.drop("D")

  return df_data


