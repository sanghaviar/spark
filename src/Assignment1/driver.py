from pyspark.sql import SparkSession
from util import *
spark = SparkSession.builder.getOrCreate()

userDf = spark.read.csv("C:\\Users\\Sanghavi\\Desktop\\sparkfiles\\user.csv",
                    inferSchema=True,header=True)
transactionDf = spark.read.csv("C:\\Users\\Sanghavi\\Desktop\\sparkfiles\\transaction.csv",
                     inferSchema=True,header=True)
user_data(userDf,transactionDf)






