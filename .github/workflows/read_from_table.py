from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import time


from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

connection_parameters = {"account":"cf15705.ap-northeast-1.aws",
                         "user":"roddicke",
                         "password":"Doris0718!",
                         "role":"ACCOUNTADMIN",
                         "warehouse":"COMPUTE_WH",
                         "database":"DEMO_DB",
                         "schema":"PUBLIC"}
session = Session.builder.configs(connection_parameters).create()

test= session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER")

test.show(5)

test = session.sql("select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER WHERE C_NATIONKEY='23'")
type(test)


test3 = test.filter(col("C_NATIONKEY")=='23').select("C_NAME")

test3.show()



test4 = test3.select("C_NATIONKEY","c_acctbal")

type(test4)

test2 = test.na.drop()
test2.show()
