import unittest
from src.Assignment2 import util
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, desc
def test_client(logsDf):
    logsDf1 = logsDf.withColumn('logLevel', split(col('value'), ',')[0]) \
        .withColumn('timeStamp', split(col('value'), ',')[1]) \
        .withColumn("DownloaderId", split(col('value'), ',')[2]) \
        .withColumn("Downloader_ID", split(col('DownloaderId'), '--')[0]).drop('DownloaderID') \
        .withColumn("Ruby_Class", split(col('value'), '--')[1]).drop('value') \
        .withColumn("RubyClass", split(col('Ruby_Class'), ' ')[1]) \
        .withColumn("Comments", split(col('Ruby_Class'), ":")[1]).drop('Ruby_Class')
    # logsDf1.show(truncate=False)
    return logsDf1

def test_number_oflines(logsDf1):
    """
    count the number of lines DataFrame contains
    """
    count1 = logsDf1.count()
    return count1
    # print( logsDf1.count())

def test_warning_messages(logsDf1):
    """
    count of number of warning messages
    """
    warn = logsDf1.filter(logsDf1.logLevel == 'WARN').count()
    return warn
    # print(warn)
def test_api_client_repo(logsDf1):
    """
    Get the count of api_client repositories
    """
    repo = logsDf1.filter(logsDf1.RubyClass == 'api_client.rb:').count()
    return repo
    # print(repo)

    # logsDf1.filter("logsDf1.Comments == '%Successful request%' ").show()
    # logsDf1.filter(logsDf1.Comments == '%Successful request%').show()
def test_https_request(logsDf1):
    """
    which client did most HTTPS request
    """
    req = logsDf1.filter(col("Comments").contains("https")).groupBy('Downloader_ID').count()
    httpReq = req.select('Downloader_ID', 'count').orderBy(desc('count')).first()

    # print(httpReq['Downloader_ID'], httpReq['count'])
    return (httpReq['Downloader_ID'], httpReq['count'])

def test_failed_https(logsDf1):
    """
    which client did most failed HTTPS request
    """
    failedReq = logsDf1.filter(col("Comments").contains("Failed request")).groupBy('Downloader_ID', 'Comments').count()
    # print(failedReq)
    df5 = failedReq.select('Downloader_ID', 'count').orderBy(desc('count')).first()
    # print(df5['Downloader_ID'], df5['count'])
    # return (df5['Downloader_ID'], df5['count'])
    return df5

spark = SparkSession.builder.master("local[5]").appName("Spark Assignment").getOrCreate()
test_logsDf = spark.read.text("C:\\Users\\Sanghavi\\Desktop\\sparkfiles\\test1_ghtorrent-logs.txt")

fun1 = test_client(test_logsDf)
fun2 = test_number_oflines(fun1)
fun3 = test_warning_messages(fun1)
fun4 = test_api_client_repo(fun1)
fun5 = test_https_request(fun1)
fun6 = test_failed_https(fun1)
class MyTestCase(unittest.TestCase):
    def test_case1(self):
        expected_output = test_client(test_logsDf)
        actual_output = util.client(test_logsDf)
        self.assertEqual(actual_output.collect(),expected_output.collect())

    def test_case2(self):
        expected_output = test_number_oflines(fun1)
        actual_output = util.number_oflines(fun1)
        self.assertEqual(actual_output,expected_output)


    def test_case3(self):
        expected_output = test_warning_messages(fun1)
        actual_output = util.warning_messages(fun1)
        self.assertEqual(actual_output,expected_output)

    def test_case4(self):
        expected_output = test_api_client_repo(fun1)
        actual_output = util.api_client_repo(fun1)
        self.assertEqual(actual_output,expected_output)
    def test_case5(self):
        expected_output = test_https_request(fun1)
        actual_output = util.https_request(fun1)
        self.assertEqual(actual_output,expected_output)

    def test_case6(self):
        expected_output = test_failed_https(fun1)
        actual_output = util.failed_https(fun1)
        self.assertEqual(actual_output,expected_output)

if __name__ == '__main__':
    unittest.main()
