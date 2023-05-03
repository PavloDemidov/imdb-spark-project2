import pyspark.sql.types as t


path_akas = 'imdb-data/title.akas.tsv.gz'
path_task1 = 'imdb_result1/task1'
path_name_basics = 'imdb-data/name.basics.tsv.gz'
path_task2 = 'imdb-result2/task2'

akas_schema = t.StructType([t.StructField('titleId', t.StringType(), False),
                            t.StructField('ordering', t.IntegerType(), True),
                            t.StructField('title', t.StringType(), True),
                            t.StructField('region', t.StringType(), True),
                            t.StructField('language', t.StringType(), True),
                            t.StructField('types', t.StringType(), True),
                            t.StructField('attributes', t.StringType(), True),
                            t.StructField('isOriginalTitle', t.BooleanType(), True)
                            ])

name_basics_schema = t.StructType([t.StructField('nconst',t.StringType(),False),
                                   t.StructField('primaryName', t.StringType(), True),
                                   t.StructField('birthYear', t.IntegerType(), True),
                                   t.StructField('deathYear', t.IntegerType(), True),
                                   t.StructField('primaryProfession', t.StringType(), True),
                                   t.StructField('knownForTitles', t.StringType(), True),
                                   ])
