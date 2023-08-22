from pyspark.sql import SparkSession
from util import *
spark = SparkSession.builder.getOrCreate()

userDf = spark.read.csv("C:\\Users\\Sanghavi\\Desktop\\sparkfiles\\user.csv",
                    inferSchema=True,header=True)
transactionDf = spark.read.csv("C:\\Users\\Sanghavi\\Desktop\\sparkfiles\\transaction.csv",
                     inferSchema=True,header=True)

fun1 = left_join(userDf,transactionDf)
fun2 = user_data(userDf,fun1)
fun3 = products_brought(fun1,userDf,transactionDf)
fun4 = spending(transactionDf)






