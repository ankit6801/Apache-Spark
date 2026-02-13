#Pyspark Module for booking analysis.

def read_booking_summary(spark):
    file_schema = "booked_by string, booking_date string, revenue double"
    summary_df = (
        spark.read.format("csv")
        .option("header","true")
        .schema(file_schema)
        # .load(filepath)
        .load("/Volumes/dev_catalog/spark_db/datasets/spark_programming/data/booking-summary.csv")
        )
    return summary_df


def top_3_revenue(summary_dff):
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col,rank

    window_spec = (
        Window.partitionBy(col("booked_by"))
        .orderBy(col("revenue").desc())
        .rowsBetween(Window.unboundedPreceding,Window.currentRow)
    )

    result_df = (
        summary_dff.withColumn("rank",rank().over(window_spec))
        .where(col('rank') <= 3)
        
    )
    return result_df


def PrintName():
    print("Ankit Kumar")
    print('Hello Learning Spark')

if __name__ == "__main__":
    PrintName()

