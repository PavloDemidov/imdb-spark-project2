from columns import *
import pyspark.sql.functions as f

def task4 (df_princ, df_name, df_title, path_to_save):

    """Отримайте імена людей, відповідні фільми/серіали та персонажі,
     які вони грали у тих фільмах
     Аргументи:
         df_princ: фрейм даних принципалів
         df_name: назва кадру даних
         df_title: фрейм даних заголовка
         path_to_save: шлях для збереження набору даних
     Результат:
         out_df:  сформований набір даних
    """
    df_princ = df_princ.drop(job, ordering).filter(f.col(category) == 'actor')
    df_title = df_title.drop(originalTitle,
                             isAdult,
                             startYear,
                             endYear,
                             runtimeMinutes,
                             genres).filter((f.col(titleType) == 'movie') |
                                            (f.col(titleType) == 'tvMovie') |
                                            (f.col(titleType)=='tvSeries') |
                                            (f.col(titleType)=='tvMiniSeries'))
    out_df = df_name.join(df_princ, on=nconst, how='inner').drop(birthYear,
                                                                   deathYear,
                                                                   primaryProfession,
                                                                   knownForTitles)
    out_df = out_df.join(df_title, on=tconst, how='inner').select(f.col(primaryName),
                                                                   f.col(primaryTitle),
                                                                   f.col(characters),)
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df