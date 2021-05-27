from pyspark.sql import SparkSession, SQLContext
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from flask_sqlalchemy import sqlalchemy
from sqlalchemy.orm.exc import NoResultFound
import os


appName = "PySpark PostgreSQL Example"
master = "local"

# sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages --packages org.postgresql:postgresql:42.2.10 pyspark-shell'

# spark = SparkSession.builder.master(master).appName(appName).config("spark.driver.extraClassPath", sparkClassPath).getOrCreate()

spark = SparkSession.builder.master(master).appName(appName).config("spark.jars", "/home/technology/Downloads/postgresql-42.2.10.jar").getOrCreate()

jdbcDF = spark.read.format("jdbc").options(
         url='jdbc:postgresql://localhost:5432/test', 
         dbtable='master_data_entry',
         user='postgres',
         password='Info123*',
         driver='org.postgresql.Driver').load()

jdbcDF.printSchema()
print("/////////",jdbcDF)

for row in jdbcDF.collect():
    print("inside for")
    print(row)