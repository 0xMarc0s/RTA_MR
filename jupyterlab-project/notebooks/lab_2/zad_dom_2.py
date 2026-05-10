from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    desc,
    round as _round,
    to_timestamp,
    window,
)


def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Lab2HomeworkSolution")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "Europe/Warsaw")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_transactions(spark: SparkSession) -> DataFrame:
    df = spark.read.json("data/transactions_10k.jsonl")
    return df.withColumn("timestamp", to_timestamp(col("timestamp")))


def gdansk_lowest_hourly_average(df: DataFrame) -> DataFrame:
    return (
        df.filter(col("store") == "Gdańsk")
        .groupBy(window("timestamp", "1 hour"))
        .agg(
            count("tx_id").alias("liczba_tx"),
            _round(avg("amount"), 2).alias("srednia_PLN"),
        )
        .select(
            col("window.start").alias("od"),
            col("window.end").alias("do"),
            "liczba_tx",
            "srednia_PLN",
        )
        .orderBy(col("srednia_PLN").asc(), col("od").asc())
        .limit(1)
    )


def category_counts_0900_0930(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(window("timestamp", "30 minutes"), "category")
        .agg(count("tx_id").alias("liczba_tx"))
        .select(
            col("window.start").alias("od"),
            col("window.end").alias("do"),
            "category",
            "liczba_tx",
        )
        .filter(
            (date_format(col("od"), "HH:mm") == "09:00")
            & (date_format(col("do"), "HH:mm") == "09:30")
        )
        .orderBy("category")
    )


def peak_15_minute_window(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(window("timestamp", "15 minutes"))
        .agg(count("tx_id").alias("liczba_tx"))
        .select(
            col("window.start").alias("od"),
            col("window.end").alias("do"),
            "liczba_tx",
        )
        .orderBy(desc("liczba_tx"), col("od").asc())
        .limit(1)
    )


def main() -> None:
    spark = create_spark()
    try:
        df = load_transactions(spark)

        print("\n1. Godzina, w której sklep Gdańsk miał najniższą średnią kwotę:")
        gdansk_lowest_hourly_average(df).show(truncate=False)

        print("\n2. Liczba transakcji per kategoria w oknie 09:00-09:30:")
        category_counts_0900_0930(df).show(truncate=False)

        print("\n3. Ćwierćgodzina ze szczytem liczby transakcji:")
        peak_15_minute_window(df).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
