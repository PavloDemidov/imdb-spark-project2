from columns import *
import pyspark.sql.functions as f

def task3 (in_df, path_to_save):
    """Отримати назви всіх фільмів, які тривають більше 2 годин
         Аргументи:
          in_df: akas dataframe
          path_to_save: шлях до теки  збереження вихідних результатів
         Результат:
          out_df:  посилання на сформований набір даних
    """


    out_df = in_df.filter((f.col(titleType) == 'movie') & (f.col(runtimeMinutes) > 120)).select(f.col(originalTitle))
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df