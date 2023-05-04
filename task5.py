from columns import *
import pyspark.sql.functions as f

def task5(df_akas, df_title, path_to_save):
    """Отримайте інформацію про те, скільки фільмів/серіалів для дорослих є
     в регіоні. Отримайте 100 найкращих із них від регіону з найбільшою кількістю
     до регіону з найменшою
     Аргументи:
         df_akas: title akas dataframe
         df_title: title dataframeа
         path_to_save: шлях до теки  збереження вихідних результатів
     Результат:
         out_df:  посилання на сформований набір даних
    """

    df_akas = df_akas.filter(f.col(region).isNotNull()).select(f.col(titleId).alias('tconst'),
                                                               f.col(region)).dropDuplicates()
    df_title = df_title.select(tconst,
                               primaryTitle,
                               isAdult).filter(f.col(isAdult) == 1)
    out_df = df_akas.join(df_title, on=tconst, how='inner')
    out_df = out_df.groupBy(region).count().orderBy('count', ascending=False).limit(100)
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df