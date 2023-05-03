import os
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

from read_write import *
from settings import *
from task1 import *

from task2 import *
from task3 import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main():
    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('task app')
                     .config(conf=SparkConf())
                     .getOrCreate())

    df1 = read_akas(spark_session, path_akas, akas_schema)
    df1.show()
    task1(df1, path_task1)
    df2 = read_name_basics(spark_session, path_name_basics, name_basics_schema)
    df2.show()
    task2(df2, path_task2)
    df3 = read_title_basics(spark_session, path_title_basics, title_basics_schema)
    df3.show()
    task3(df3, path_task3)


if '__main__' == __name__:
    main()

