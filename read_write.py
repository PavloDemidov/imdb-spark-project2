from columns import *
import pyspark.sql.functions as f

def read_df(session, path, schema):
    """ Читання даних у фрейм даних із CSV
     Аргументи:
        session: spark сесія
        path: шлях до файлу csv
        schema: схема набору даних

    Результат:
        dataframe- датафрейм
    """
    df = session.read.csv(path, header=True, sep='\t', schema=schema, nullValue=r'\N')
    return df


def read_akas(session, path, schema):
    """ Читання та підготовка даних на основі набору даних akas
     Аргументи:
        session: spark сесія
        path: шлях до файлу csv
        schema: схема набору даних

    Результат:
        dataframe- датафрейм
    """
    df = read_df(session, path, schema)
    df = df.select('*',
                   f.split(f.col(types), ',').alias('ty'),
                   f.split(f.col(attributes), ',').alias('att')).drop(types, attributes)
    df = df.withColumnRenamed('ty', types)
    df = df.withColumnRenamed('att', attributes)
    return df

def read_name_basics(session, path, schema):
    """ Чтение и базовая подготовка данных на основі данных name.basics
     Аргументы:
         session: spark сесія
         path: путь к CSV-файлу
         schema: схема набора данных

     Результат:
          dataframe - датафрейм
    """
    df = read_df(session, path, schema)
    df = df.select('*',
                   f.split(f.col(primaryProfession), ',').alias('pP'),
                   f.split(f.col(knownForTitles), ',').alias('kFT')).drop(knownForTitles, primaryProfession)
    df = df.withColumnRenamed('kFT', knownForTitles)
    df = df.withColumnRenamed('pP', primaryProfession)
    return df


def read_title_basics(session, path, schema):
    """ Читання та підготовка баз даних на основі набору даних title_basics
    Аргументи:
         session: spark сесія
         path: путь к CSV-файлу
         schema: схема набора данных

     Результат:
         dataframe - датафрейм
    """

    df = read_df(session, path, schema)
    df = df.select('*',
                   f.split(f.col(genres), ',').alias('gen')).drop(genres)
    df = df.withColumnRenamed('gen', genres)
    return df
