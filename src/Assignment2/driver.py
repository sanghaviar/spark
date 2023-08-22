from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,desc
from util import *
spark = SparkSession.builder.master("local[5]").appName("Spark Assignment").getOrCreate()
logsDf = spark.read.text("C:\\Users\\Sanghavi\\Desktop\\sparkfiles\\ghtorrent-logs.txt")
# print( logs.count()) # count the number of lines DataFrame contains
# # logs.show(n= 30,truncate=False)
# client(logsDf)

fun1 = client(logsDf)
fun2 = number_oflines(fun1)
fun3 = warning_messages(fun1)
fun4 = api_client_repo(fun1)
fun5 = https_request(fun1)
fun6 = failed_https(fun1)












