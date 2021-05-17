package com.schiphol

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{expr, row_number, to_timestamp, window}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import com.schiphol.Functions.selectSchema
import com.schiphol.storage._

object PopularAirportsWindowAgg extends SparkJob {

  override def appName: String = "PopularAirportsWindowAgg"
  val LOG: Logger = LoggerFactory.getLogger(appName)

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {

    import spark.implicits._

    val topNSamples = config.topNSamples.getOrElse("10")
    val triggerIntervalSeconds = config.triggerIntervalSeconds.getOrElse(10L)
    val watermarkIntervalSeconds =
      config.watermarkIntervalSeconds.getOrElse("15")
    val windowIntervalSeconds =
      config.windowIntervalSeconds.getOrElse("10")
    val windowSlideDurSeconds =
      config.windowSlideDurSeconds.getOrElse("5")
    val outputPath = s"/usr/local/data/prod/output/task3-stream-windowing"
    val outputFile = s"task3-out-result.dat"

    val routes = storage.flightRoutesStream

    val result = transform(
      spark,
      routes,
      watermarkIntervalSeconds,
      windowIntervalSeconds,
      windowSlideDurSeconds
    )

    val query = result.toDF.writeStream.foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
      val batchDs = batchDF.as[SourceAirportWindowedCountSchema]
      // we added additional transformation of data in foreachBatch because is not supported by Spark Aif we leave in `transform` function
      val result = popularAirportsForEachWindow(spark, batchDs, topNSamples)

      // let's create only 1 csv (instead writeToCsv) file and append content of all windows
      // TODO: check writeToCsv with coalesce(1)
      storage.writeStringToFile(
        outputPath,
        outputFile,
        result.mkString("\n"),
        append = true
      )
    }

    // Update mode: the watermark is used to drop too old aggregates. Will output only updated rows since the last trigger
    query
      .option("truncate", false)
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime(triggerIntervalSeconds, TimeUnit.SECONDS)) // trigger each 10 seconds, will read 1 not processed CSV
      .start()
      .awaitTermination()

  }

  def transform(
      spark: SparkSession,
      routes: Dataset[FlightRouteSchema],
      watermarkIntervalSeconds: String,
      windowIntervalSeconds: String,
      windowSlideDurSeconds: String
    ): Dataset[SourceAirportWindowedCountSchema] = {

    import spark.implicits._

    // current_timestamp() is evaluated prior to serialisation so it will always give same ts,
    // we need TS when we read exact values from the stream.
    // We use watermark to remove from state the late events.
    val outDf = routes
      .withColumn(
        "nowTs",
        to_timestamp(expr("reflect('java.time.LocalDateTime', 'now')"))
      )
      .withWatermark("nowTs", s"$watermarkIntervalSeconds seconds")
      .withColumn(
        "window",
        window(
          $"nowTs",
          s"$windowIntervalSeconds seconds",
          s"$windowSlideDurSeconds seconds"
        )
      )
      .groupBy($"sourceAirport", $"sourceAirportId", $"window")
      .count()
      .withColumnRenamed("count", "countRoutes")
      .withColumn("windowStart", to_timestamp($"window.start"))
      .withColumn("windowEnd", to_timestamp($"window.end"))
      .select(
        $"sourceAirport",
        $"sourceAirportId",
        $"windowStart",
        $"windowEnd",
        $"countRoutes"
      )

    val outDs = selectSchema[SourceAirportWindowedCountSchema](outDf)
      .as[SourceAirportWindowedCountSchema]

    LOG.info(s"Transform of stream with sliding window")

    outDs
  }

  def popularAirportsForEachWindow(
      spark: SparkSession,
      batchDs: Dataset[SourceAirportWindowedCountSchema],
      topNSamples: String
    ): List[String] = {

    import spark.implicits._
    // We separated some of transform steps in this function because are not supported by Spark api if we leave in transform function.
    // These transformations (selecting topN samples) are executed in foreachBatch over data of each micro-batch.
    val wTopN = Window
      .partitionBy("windowStart") // we select top popular for each sliding window
      .orderBy($"countRoutes".desc, $"sourceAirport")

    val popularAirportsWindow = batchDs
      .withColumn(
        "rn",
        row_number().over(wTopN)
      )
      .filter($"rn" <= topNSamples)
      .drop("rn")

    val outputDs =
      selectSchema[SourceAirportWindowedCountSchema](popularAirportsWindow)
        .as[SourceAirportWindowedCountSchema]

    // we use collect but we are sure that we don't have big data so will not crash the spark driver
    val output =
      outputDs
        .collect()
        .map { row: SourceAirportWindowedCountSchema =>
          val windowStart = row.windowStart
          val windowEnd = row.windowEnd
          val sourceAirport = row.sourceAirport
          val sourceAirportId = row.sourceAirportId
          val countRoutes = row.countRoutes
          s"$windowStart,$windowEnd,$sourceAirportId,$sourceAirport,$countRoutes"
        }
        .toList
        .distinct

    LOG.info(s"Selecting top $topNSamples popular airports from for each sliding window")

    output
  }

}
