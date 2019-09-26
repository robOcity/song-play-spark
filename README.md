# PySpark ETL pipeline using AWS EMR and AWS S3

## From logs to a star-schema  

## Citations

1. [Removing duplicate columns after a DF join in Spark](https://stackoverflow.com/questions/46944493/removing-duplicate-columns-after-a-df-join-in-spark) - Solution to duplicate column name resulting from a join is to either:

    * join using a list `df1.join(df2, ['id']).show()` joining on the common `id` column
    * leave the columns named indendically and provide an alias for each table:
    `df1.alias("a").join(df2.alias("b"), df1['id'] == df2['id']).select("a.id", "a.val1", "b.val2").show()`
