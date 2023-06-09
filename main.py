import os
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

from read_write import *
from settings import *
from task1 import *
from task2 import *
from task3 import *
from task4 import *
from task5 import *
from task6 import *
from task7 import *
from task8 import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main():
    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('task app')
                     .config(conf=SparkConf())
                     .getOrCreate())

    movies_df1 = read_akas(spark_session, path_akas, akas_schema)
    movies_df1.show()
    task1(movies_df1, path_task1)
    movies_df2 = read_name_basics(spark_session, path_name_basics, name_basics_schema)
    movies_df2.show()
    task2(movies_df2, path_task2)
    movies_df3 = read_title_basics(spark_session, path_title_basics, title_basics_schema)
    movies_df3.show()
    task3(movies_df3, path_task3)
    movies_df4 = read_df(spark_session, path_principals, principals_schema)
    movies_df4.show()
    task4(movies_df4, movies_df2, movies_df3, path_task4)
    task5(movies_df1, movies_df3, path_task5)#
    movies_df5 = read_df(spark_session, path_episode, episode_schema)
    task6(movies_df5, movies_df3, path_task6)
    movies_df6 = read_df(spark_session, path_ratings, ratings_schema)
    task7(movies_df6,  movies_df3, path_task7)
    task8(movies_df6, movies_df3, path_task8)


if '__main__' == __name__:
    main()

