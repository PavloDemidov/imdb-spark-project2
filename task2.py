from columns import *
import pyspark.sql.functions as f


def task2(in_df, path_to_save):
    """Получить список имен людей, родившихся в 19 веке
     Аргументы:
         in_df: базовый фрейм данных имени
         path_to_save: путь для сохранения фрейма данных
     Результат:
         out_df: сформированный кадр данных
    """

    out_df = in_df.filter(f.col(birthYear).between(1800, 1899)).select(f.col(primaryName))
    out_df.write.csv(path_to_save, header=True, mode='overwrite')

    return out_df
