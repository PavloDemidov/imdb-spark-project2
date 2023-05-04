from columns import *
import pyspark.sql.functions as f
import pyspark.sql.types as t

def task7(df_ratings, df_title, path_to_save):
    """Отримайте 10 назв найпопулярніших фільмів/серіалів за кожне десятиліття

     Аргументи:
         df_ratings: ratings dataframe
         df_title: title dataframe
         path_to_save: шлях до теки  збереження вихідних результатів
     Результат:
         out_df: посилання на сформований набір даних
    """
    df_title = df_title.select(tconst,
                             primaryTitle,
                             startYear,
                             endYear)
    # з початку і кінця року оцінити десятиліття
    df_title = (df_title.withColumn('decade',
                                    f.array(f.floor(f.col(startYear)/10)*10,
                                            (f.when(f.col(endYear).isNotNull(),
                                                    (f.ceil(f.col(endYear)/10)*10-1))
                                             .otherwise((f.floor(f.col(startYear)/10)*10+9)))
                                            )))
    # знайти мінімальний і максимальний рік із набору даних
    min_year=df_title.select(f.min(startYear)).first()[0]
    max_year1=df_title.select(f.max(startYear)).first()[0]
    max_year2=df_title.select(f.max(endYear)).first()[0]
    join_df = df_title.join(df_ratings, on=tconst, how='inner')
    max_year = max_year1 if max_year1>max_year2 else max_year2
    start = min_year//10*10
    end = (max_year//10)*10
    dec_num=(end-start)//10
    # за визначенням популярності: 1. факт, що щось або хтось подобається, подобається чи підтримується багатьма людьми
    out_df = (join_df.select(primaryTitle,
                           averageRating,
                           numVotes,
                           'decade').where(
                            (f.col(startYear).between(start, start+9)) |
                            (f.col(endYear).between(start, start+9)))
            .orderBy(f.col(averageRating).desc(),
                     f.col(numVotes).desc()).limit(10)
            .dropDuplicates())
    start = start+10
    for _ in range(1,dec_num):
        decad_df = (join_df.select(primaryTitle,
                                   averageRating,
                                   numVotes,
                                   'decade').where(
                                            (f.col(startYear).between(start, start+9)) |
                                            (f.col(endYear).between(start, start+9)))
                    .orderBy(f.col(averageRating).desc(),
                             f.col(numVotes).desc()).limit(10)
                    .dropDuplicates())
        out_df=out_df.union(decad_df)
        start = start + 10
    # приведення, оскільки джерело даних CSV не підтримує тип даних масиву
    out_df=out_df.withColumn('decade', df_title.decade.cast(t.StringType()))
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df