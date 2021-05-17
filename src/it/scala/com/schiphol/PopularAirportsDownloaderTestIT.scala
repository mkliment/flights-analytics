package com.schiphol

import scala.sys.process.ProcessLogger
import scala.sys.process._

import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.schiphol.Functions.selectSchema
import com.schiphol.storage.{FlightRouteSchema, Storage}
import com.schiphol.test.SharedSparkSession.spark

class PopularAirportsDownloaderTestIT extends AnyFunSuite with Matchers with Logging {

  val testTargetPath = s"data/test/input/downloaderBatch"

  // TODO: add beforeAll and afterAll each test (or after all tests?!) cleanup process which will clean produced files by tests
  // now we use ugly cleaner.
  // while CI/CD data which we use in test stays in builder image base, not in final image
  def cleanTestDir(testTargetPath: String) = {
    LOGGER.info(s"Deletion with recursive on following path: $testTargetPath")
    s"rm -r ${testTargetPath}" lineStream_! ProcessLogger(
      LOGGER.info,
      LOGGER.info
    )
  }

  class LocalStorage(spark: SparkSession) extends Storage {

    // TODO: optimise class LocalStorage in IT tests to prevent duplication in each test
    import spark.implicits._
    spark.sparkContext.setLogLevel("INFO")

    override def flightRoutes: Dataset[FlightRouteSchema] = {
      routesDfColumnsMapper(spark.read.csv("not_used_in_this_test"))
    }

    private def readCsvStream(location: String, schema: StructType, header: String) = {
      spark.readStream.schema(schema).csv(location)
    }

    override def flightRoutesStream: Dataset[FlightRouteSchema] = {
      readCsv("not_used_in_this_test", "true").as[FlightRouteSchema]
    }

    override def writeStringToFile(outputPath: String, outputFile: String, content: String, append: Boolean = true): Unit = {
      println(s"not_used_in_this_test")
    }

    private def readCsv(location: String, header: String) = {
      spark.read
        .option("header", header)
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .csv(location)
    }

    private def routesDfColumnsMapper(df: DataFrame) = {
      df.select(
          FlightRouteSchema.columnsMap.keys.toList
            .map(c => col(c).as(FlightRouteSchema.columnsMap.getOrElse(c, c))): _*
        )
        .as[FlightRouteSchema]
    }

    override def remoteFlightRoutes: Dataset[FlightRouteSchema] = {
      spark.sparkContext.addFile(
        "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
      )
      val remoteFlightRoutesDf =
        readCsv(SparkFiles.get("routes.dat"), "false")

      routesDfColumnsMapper(remoteFlightRoutesDf)
    }

    override def writeToCsv(ds: Dataset[_], saveMode: String, location: String): Unit = {
      ds.write
        .option("header", "true")
        .mode("overwrite")
        .csv(testTargetPath)
    }

  }

  test("apps using Storage should read HTTP csv and write to disk") {
    import spark.implicits._
    val config = UsageConfig()
    PopularAirportsDownloader.run(spark, config, new LocalStorage(spark))

    // this is going maybe in normal unit testing, not IT test, but still lets check if output csv (overwritten) is correct
    // read all csv files, content is small we can afford in this assignment and IT test, if time execution of IT tests is huge we will
    // find other solution
    val savedOutputDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("samplingRatio", "0.1")
      .option("delimiter", ",")
      .csv("data/test/input/downloaderBatch")

    val savedOutputDs = selectSchema[FlightRouteSchema](savedOutputDf)
      .as[FlightRouteSchema]
    assert(savedOutputDs.count === 67663)

    val checkOutputSavedRows =
      savedOutputDs
        .filter($"sourceAirport" === "SIN")
        .collect
        .map(r => r.sourceAirportId)
        .head
    assert(checkOutputSavedRows === "3316")
  }

  test("dummy test to clean directory used for IT tests") {
    cleanTestDir(testTargetPath)
  }

}
