package com.schiphol

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{FileWriter, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit

import scala.sys.process.ProcessLogger
import scala.sys.process._

import com.schiphol.Functions.selectSchema
import com.schiphol.PopularAirportsWindowAgg.popularAirportsForEachWindow
import com.schiphol.storage.{FlightRouteSchema, SourceAirportWindowedCountSchema, Storage}
import com.schiphol.test.SharedSparkSession.spark

class PopularAirportsWindowAggTestIT extends AnyFunSuite with Matchers with Logging {

  val topNSamples = "2"
  val triggerIntervalSeconds = 15
  val watermarkIntervalSeconds = "1"
  val windowIntervalSeconds = "10"
  val windowSlideDurSeconds = "5"
  val outputPath = s"data/test/out/outWindowAgg"
  val outputFile = s"popularAirportsPerWindow.dat"

  def cleanTestDir(testTargetPath: String) = {
    LOGGER.info(s"Deletion with recursive on following path: $testTargetPath")
    s"rm -r ${testTargetPath}" lineStream_! ProcessLogger(
      LOGGER.info,
      LOGGER.info
    )
  }

  class LocalStorage(spark: SparkSession) extends Storage {

    import spark.implicits._

    val checkpointPath = s"data/test/out/checkpoints/$randomUUID/"
    spark.conf.set(
      "spark.sql.streaming.checkpointLocation",
      new Path(checkpointPath).toString
    )
    spark.sparkContext.setLogLevel("ERROR")

    private def readCsv(location: String, header: String) = {
      spark.read.option("header", header).csv(location)
    }

    override def remoteFlightRoutes: Dataset[FlightRouteSchema] = {
      routesDfColumnsMapper(spark.read.csv("not_used_in_this_test"))
    }

    override def flightRoutes: Dataset[FlightRouteSchema] = {
      readCsv("not_used_in_this_test", "true").as[FlightRouteSchema]
    }

    override def writeToCsv(ds: Dataset[_], saveMode: String, location: String): Unit = {
      ds.write
        .csv("not_used_in_this_test")
    }

    private def routesDfColumnsMapper(df: DataFrame) = {
      df.select(
          FlightRouteSchema.columnsMap.keys.toList
            .map(c => col(c).as(FlightRouteSchema.columnsMap.getOrElse(c, c))): _*
        )
        .as[FlightRouteSchema]
    }

    private def readCsvStream(location: String, schema: StructType, header: String) = {
      spark.readStream
        .option("header", header)
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .option("maxFilesPerTrigger", 1)
        .schema(schema)
        .csv(location)
    }

    override def flightRoutesStream: Dataset[FlightRouteSchema] = {
      val schema = Encoders.product[FlightRouteSchema].schema
      val flightRoutesStreamDf =
        readCsvStream(
          "src/it/resources/sample_*.csv",
          schema,
          "true"
        )
      val flightRouteStreamDs = selectSchema[FlightRouteSchema](flightRoutesStreamDf)
        .as[FlightRouteSchema]
      flightRouteStreamDs
    }

    override def writeStringToFile(outputPath: String, outputFile: String, content: String, append: Boolean = true): Unit = {
      val fullPath = s"$outputPath/$outputFile"
      val dir = Paths.get(outputPath)
      if (!Files.exists(dir)) Files.createDirectories(dir)
      val fileWriter = new FileWriter(fullPath, append)
      val printWriter = new PrintWriter(fileWriter)
      printWriter.println(content)
      fileWriter.close()
    }

  }

  test(
    "window agg app using Storage should read stream from disk and write to disk"
  ) {
    import spark.implicits._
    val storage = new LocalStorage(spark)
    val routes = storage.flightRoutesStream

    val result = PopularAirportsWindowAgg.transform(
      spark,
      routes,
      watermarkIntervalSeconds,
      windowIntervalSeconds,
      windowSlideDurSeconds
    )

    val query = result.toDF.writeStream.foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
      val batchDs = batchDF.as[SourceAirportWindowedCountSchema]
      val result = popularAirportsForEachWindow(spark, batchDs, topNSamples)

      storage.writeStringToFile(
        outputPath,
        outputFile,
        result.mkString("\n"),
        append = true
      )
    }

    query
      .option("truncate", value = false)
      //      .format("console")
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime(triggerIntervalSeconds, TimeUnit.SECONDS))
      .start()
      .awaitTermination(
        180000
      ) // we need in IT test to limit execution of the test, increase if needed

  }

  test("dummy test to clean directory used for IT tests") {
    cleanTestDir(outputPath)
  }

}
