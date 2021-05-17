package com.schiphol

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, to_timestamp, window}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import com.schiphol.storage._
import com.schiphol.Functions.selectSchema
import org.slf4j.{Logger, LoggerFactory}

object PopularAirportsStream extends SparkJob {

  override def appName: String = "PopularAirportsStream"
  val LOG: Logger = LoggerFactory.getLogger(appName)

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {
    import spark.implicits._

    val topNSamples = config.topNSamples.getOrElse("10")

    val routes = storage.flightRoutesStream

    val result = transform(spark, routes, topNSamples)

    val query = result.toDF.writeStream.foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
      // Many operations are not supported in structured streaming because Spark doesnt support incremental plans in those cases, that's
      // why we apply those operations on each micro-batch otuput.
      //we save output to file which supports only append mode, se that's why we work on micro batch level, we save each micro-batch
      // separately
      storage.writeToCsv(
        batchDF.as[SourceAirportWindowedCountSchema],
        "overwrite",
        s"/usr/local/data/prod/output/task2-stream"
      )
    }

    // Trigger once will process all files at once as required in assignment, will ignore maxFilesPerTrigger
    // which we use in PopularAirportsWindowAgg spark job.
    // Complete Mode: In this mode, Spark will output all the rows it has processed on trigger once which are not dropped by watermark
    query
      .option("truncate", false)
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.Once())
      .start()
      .awaitTermination()

  }

  def transform(spark: SparkSession, routes: Dataset[FlightRouteSchema], topNSamples: String): Dataset[SourceAirportWindowedCountSchema] = {

    import spark.implicits._

    // current_timestamp = Returns the current timestamp at the start of query evaluation as a timestamp column.
    // That means that we will have same value for windows so we can use for window aggregation in this spark job
    val outDf = routes
      .withColumn(
        "readTsUtc",
        current_timestamp()
      )
      .withWatermark(
        "readTsUtc",
        "10 minutes"
      ) //we define huge window size to cover all records at once, we can move the values for window as input parameters of the jobs, but
      // is not useful as in next stream app PopularAirportsWindowAgg
      .withColumn("window", window($"readTsUtc", s"120 minutes"))
      .groupBy($"sourceAirport", $"sourceAirportId", $"window")
      .count()
      .withColumnRenamed("count", "countRoutes")
      .withColumn("windowStart", to_timestamp($"window.start")) // flat structure in output
      .withColumn("windowEnd", to_timestamp($"window.end"))
      .orderBy($"window", $"countRoutes".desc) // sorting is possible in complete output mode, but file sink prevents that as needs
      // append mode, that's why I use foreachBatch above
      .select(
        $"sourceAirport",
        $"sourceAirportId",
        $"windowStart",
        $"windowEnd",
        $"countRoutes"
      )
      .limit(topNSamples.toInt)

    val outDs = selectSchema[SourceAirportWindowedCountSchema](outDf)
      .as[SourceAirportWindowedCountSchema]

    LOG.info(s"Transform of stream with trigger once is finished") // just example of log line

    outDs
  }

}
