package com.schiphol

import org.apache.spark.sql.{Dataset, SparkSession}
import com.schiphol.storage._
import org.apache.spark.sql.functions.{count, row_number}
import com.schiphol.Functions.selectSchema

object PopularAirports extends SparkJob {

  override def appName: String = "PopularAirports"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {

    val topNSamples = config.topNSamples.getOrElse("10")

    val routes = storage.flightRoutes

    val result = transform(spark, routes, topNSamples)

    storage.writeToCsv(result, "overwrite", s"/usr/local/data/prod/output/task1-batch")
  }

  def transform(spark: SparkSession, routes: Dataset[FlightRouteSchema], topNSamples: String): Dataset[SourceAirportCountSchema] = {
    import spark.implicits._
    import org.apache.spark.sql.expressions.Window
    val wo = Window.orderBy($"countRoutes".desc, $"sourceAirport".asc)

    val outDf = routes
      .groupBy($"sourceAirport", $"sourceAirportId")
      .agg(count($"destinationAirport").alias("countRoutes"))
      .withColumn("rn", row_number().over(wo))
      .filter($"rn" <= topNSamples.toLong)
      .drop("rn")

    val outDs = selectSchema[SourceAirportCountSchema](outDf)
      .as[SourceAirportCountSchema]
      .coalesce(
        1
      ) //we have small data so let's create only 1 csv file

    outDs
  }

}
