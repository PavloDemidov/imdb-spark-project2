import pyspark.sql.types as t


path_akas = 'imdb-data/title.akas.tsv.gz'
path_task1 = 'imdb_result/task1'


akas_schema = t.StructType([t.StructField('titleId', t.StringType(), False),
                            t.StructField('ordering', t.IntegerType(), True),
                            t.StructField('title', t.StringType(), True),
                            t.StructField('region', t.StringType(), True),
                            t.StructField('language', t.StringType(), True),
                            t.StructField('types', t.StringType(), True),
                            t.StructField('attributes', t.StringType(), True),
                            t.StructField('isOriginalTitle', t.BooleanType(), True)
                            ])

