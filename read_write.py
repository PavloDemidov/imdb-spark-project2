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
    """ Читання та підготовка даних для набору даних akas
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
