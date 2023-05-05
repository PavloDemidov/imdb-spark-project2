from columns import *
import pyspark.sql.functions as f

def task6(df_episode, df_title, path_to_save):
    """Отримати інформацію про кількість епізодів у кожному серіалі.
    Отримайте їх 50, починаючи з серіалу з найбільшою кількістю  епізодів
     Аргументи:
         df_episode: episode dataframe
         df_title: title dataframe
         path_to_save: шлях до теки  збереження вихідних результатів
     Результат:
         out_df:  посилання на сформований набір даних
    """
    df_title=df_title.select(f.col(tconst).alias(parentTconst),
                             primaryTitle).filter((f.col(titleType)=='tvSeries'))
    out_df = df_title.join(df_episode, on=parentTconst, how='inner')
    out_df = out_df.groupBy(primaryTitle).count().orderBy('count', ascending=False).limit(50)
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df
