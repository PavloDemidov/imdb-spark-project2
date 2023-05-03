from columns import *
import pyspark.sql.functions as f


def task1(in_df, path_to_save):
    """Отримати всі назви серіалів/фільмів що, доступні українською мовою
     Аргументи:
         in_df: фрейм даних
         path_to_save: шлях для збереження кадру даних
     Повернення:
         out_df: сформований набір даних
    """
    out_df = in_df.filter(f.col(region) == 'UA').select(f.col(title))
    out_df.write.csv(path_to_save, header=True, mode='overwrite')

    return out_df
