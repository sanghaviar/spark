import unittest
from pyspark.sql import SparkSession
from src.Assignment1 import util
def test_left_join(userDf,transactionDf):
    left_join = transactionDf.join(userDf, userDf.user_id == transactionDf.user_id, how="left")
    return left_join

def test_user_data(userDf,left_join):
    # Count of unique locations where each product is sold
    final_df = left_join.select(userDf['location ']).distinct()
    fin = final_df.count()
    # print(fin)
    return fin

def test_products_brought(left_join,userDf,transactionDf):
    # products bought by each user.
    second = left_join.select(userDf['user_id'], transactionDf['product_description']).orderBy(transactionDf["user_id"])
    # second.show()
    return second

def test_spending(transactionDf):
    # Total spending done by each user on each product
    third = transactionDf.select("user_id", "product_description", "price").orderBy(transactionDf['user_id'])
    # third.show()
    return third
spark = SparkSession.builder.getOrCreate()
test_userDf = spark.read.csv("C:\\Users\\Sanghavi\\Desktop\\sparkfiles\\test_user2.csv",
                                inferSchema=True, header=True)
test_transactionDf = spark.read.csv("C:\\Users\\Sanghavi\\Desktop\\sparkfiles\\test_transaction2.csv",
                                       inferSchema=True, header=True)

fun1 = test_left_join(test_userDf,test_transactionDf)
fun2 = test_user_data(test_userDf,fun1)
fun3 = test_products_brought(fun1,test_userDf,test_transactionDf)
fun4 = test_spending(test_transactionDf)
class MyTestCase(unittest.TestCase):

    def test_case1(self):

        expected_output = test_left_join(test_userDf,test_transactionDf)
        actual_output  = util.left_join(test_userDf,test_transactionDf)
        self.assertEqual(actual_output.collect(),expected_output.collect())

    def test_case2(self):
        expected_output = test_user_data(test_userDf,fun1)
        actual_output = util.user_data(test_userDf, fun1)
        self.assertEqual(actual_output, expected_output)
    def test_case3(self):
        expected_output = test_products_brought(fun1,test_userDf,test_transactionDf)
        actual_output  = util.products_brought(fun1,test_userDf,test_transactionDf)
        self.assertEqual(actual_output.collect(),expected_output.collect())

    def test_case4(self):
        expected_output = test_spending(test_transactionDf)
        actual_output  = util.spending(test_transactionDf)
        self.assertEqual(actual_output.collect(),expected_output.collect())

if __name__ == '__main__':
    unittest.main()
