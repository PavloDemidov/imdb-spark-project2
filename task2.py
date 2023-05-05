from columns import *
import pyspark.sql.functions as f


def task2(in_df, path_to_save):
    """Отримати список імен людей, що народилися у 19 столітті
      Аргументи:
           in_df: name basics dataframe
           path_to_save: шлях до теки  збереження вихідних результатів
      Результат:
          out_df: посилання на сформований набір даних
    """

    out_df = in_df.filter(f.col(birthYear).between(1800, 1899)).select(f.col(primaryName))
    out_df.write.csv(path_to_save, header=True, mode='overwrite')

    return out_df
