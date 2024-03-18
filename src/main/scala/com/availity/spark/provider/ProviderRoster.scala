package com.availity.spark.provider

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  DateType,
  StringType
}

import org.apache.spark.sql.functions.{
  count,
  lit,
  array,
  collect_list,
  col,
  month,
  avg
}

object ProviderRoster {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkCSVProcessor")
      .master("local[*]")
      .getOrCreate()

    val schemaVisits = StructType(
      Seq(
        StructField("visit_id", StringType, nullable = true),
        StructField("provider_id", StringType, nullable = true),
        StructField("visit_date", DateType, nullable = true)
      )
    )

    val dfProviders = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv("./data/providers.csv")

    val columns = dfProviders.columns
    val dfVisits = spark.read.schema(schemaVisits).csv("./data/visits.csv")
    val dfProvidersVisits =
      dfProviders.join(dfVisits, Seq("provider_id"), "left")

    val groupByColumns = Seq(
      "provider_id",
      "first_name",
      "middle_name",
      "last_name",
      "provider_specialty"
    )

    // calculate total number of visits per provider
    val dfVisitCounts = dfProvidersVisits
      .groupBy(groupByColumns.map(col): _*)
      .agg(
        count("*").alias("total_visits")
      )

    // json output partitioned by provider's specialty
    dfVisitCounts.write
      .partitionBy("provider_specialty")
      .json("./output")

    // calculate total number of visits per provider per month
    val dfVisitCountsByMonth = dfProvidersVisits
      .groupBy(col("provider_id"), month(col("visit_date")).alias("month"))
      .agg(count("*").alias("total_visits"))

    dfVisitCountsByMonth.write
      .json("./output_by_month")

    spark.stop()
  }
}
