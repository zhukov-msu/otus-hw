import pyspark.sql.functions as F
from ..tools import *

def clean_dataframe(df):
    df = df.na.drop("any")  # очистка пустых значений
    df = df.dropDuplicates(['transaction_id'])
    return df


def main():
    spark = get_spark()
    schema = get_schema()

    filelist = ['2019-09-21']
    for filename in filelist:
        df = spark.read.schema(schema) \
            .option("comment", "#") \
            .option("header", False) \
            .csv(f'{filename}.txt')
        df = clean_dataframe(df)
        # write to one parquet
        (
            df
            .repartition(1)
            .write.parquet(f"s3a://{RESULT_BUCKET}/{filename}.parquet", mode="overwrite")
         )
    spark.stop()


if __name__ == "__main__":
    main()
