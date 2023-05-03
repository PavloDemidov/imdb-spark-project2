import os
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

from read_write import *
from settings import *
from task1 import *

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


if '__main__' == __name__:
    main()

