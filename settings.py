import pyspark.sql.types as t


path_akas = 'imdb-data/title.akas.tsv.gz'
path_task1 = 'imdb_result1/task1'
path_name_basics = 'imdb-data/name.basics.tsv.gz'
path_task2 = 'imdb-result2/task2'
path_title_basics = 'imdb-data/title.basics.tsv.gz'
path_task3 = 'imdb-result3/task3'
path_principals = 'imdb-data/title.principals.tsv.gz'
path_task4 = 'imdb-result4/task4'

akas_schema = t.StructType([t.StructField('titleId', t.StringType(), False),
                            t.StructField('ordering', t.IntegerType(), True),
                            t.StructField('title', t.StringType(), True),
                            t.StructField('region', t.StringType(), True),
                            t.StructField('language', t.StringType(), True),
                            t.StructField('types', t.StringType(), True),
                            t.StructField('attributes', t.StringType(), True),
                            t.StructField('isOriginalTitle', t.BooleanType(), True)
                            ])

name_basics_schema = t.StructType([t.StructField('nconst', t.StringType(),False),
                                   t.StructField('primaryName', t.StringType(), True),
                                   t.StructField('birthYear', t.IntegerType(), True),
                                   t.StructField('deathYear', t.IntegerType(), True),
                                   t.StructField('primaryProfession', t.StringType(), True),
                                   t.StructField('knownForTitles', t.StringType(), True),
                                   ])

title_basics_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                    t.StructField('titleType', t.StringType(), True),
                                    t.StructField('primaryTitle', t.StringType(), True),
                                    t.StructField('originalTitle', t.StringType(), True),
                                    t.StructField('isAdult', t.IntegerType(), True),
                                    t.StructField('startYear', t.IntegerType(), True),
                                    t.StructField('endYear', t.IntegerType(), True),
                                    t.StructField('runtimeMinutes', t.IntegerType(), True),
                                    t.StructField('genres', t.StringType(), True)
                                    ])
principals_schema=t.StructType([t.StructField('tconst', t.StringType(), False),
                                t.StructField('ordering', t.IntegerType(), True),
                                t.StructField('nconst', t.StringType(), True),
                                t.StructField('category', t.StringType(), True),
                                t.StructField('job', t.StringType(), True),
                                t.StructField('characters', t.StringType(), True),
                                ])
