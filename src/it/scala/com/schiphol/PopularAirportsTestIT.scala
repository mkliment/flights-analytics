package com.schiphol

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types.StructType

import com.schiphol.Functions.selectSchema
import com.schiphol.test.SharedSparkSession.spark
import com.schiphol.storage.{FlightRouteSchema, SourceAirportCountSchema, Storage}

class PopularAirportsTestIT extends AnyFunSuite with Matchers with Logging {

  class LocalStorage(spark: SparkSession) extends Storage {
    import spark.implicits._

    override def remoteFlightRoutes: Dataset[FlightRouteSchema] = {
      routesDfColumnsMapper(spark.read.csv("not_used_in_this_test"))
    }

    private def readCsvStream(location: String, schema: StructType, header: String) = {
      spark.readStream.schema(schema).csv(location)
    }

    override def flightRoutesStream: Dataset[FlightRouteSchema] = {
      val schema = Encoders.product[FlightRouteSchema].schema
      readCsvStream("not_used_in_this_test", schema, "true")
        .as[FlightRouteSchema]
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

    override def flightRoutes: Dataset[FlightRouteSchema] = {
      val flightRoutesDf =
        readCsv(
          "src/it/resources/sample_*.csv",
          "true"
        )
      selectSchema[FlightRouteSchema](flightRoutesDf)
        .as[FlightRouteSchema]
    }

    override def writeToCsv(ds: Dataset[_], saveMode: String, location: String): Unit = {
      ds.write
        .option("header", "true")
        .mode("overwrite")
        .csv("data/test/out/outBatch")
    }
  }

  test("apps using Storage should read and write to disk") {
    import spark.implicits._
    val config = UsageConfig(Some("2"))
    PopularAirports.run(spark, config, new LocalStorage(spark))

    // this is going maybe in normal unit testing, not IT test, but still lets check if output csv (overwritten) is correct
    val savedOutputDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv("data/test/out/outBatch")
    val savedOutputDs = selectSchema[SourceAirportCountSchema](savedOutputDf)
      .as[SourceAirportCountSchema]
    assert(savedOutputDs.count === 2)

    val checkOutputSavedRows =
      savedOutputDs
        .filter($"sourceAirport" === "AAE")
        .collect
        .map(r => r.countRoutes)
        .head
    assert(checkOutputSavedRows === 6)
  }

  // maybe this unit test is more for unit test than here in IT tests
  test("apps has correct mapping of fields while read non-header cvs file") {
    import spark.implicits._

    val storage = new LocalStorage(spark)
    val inputDs = storage.flightRoutes
    val result = PopularAirports.transform(spark, inputDs, "2")

    val inputRowForCheck =
      inputDs
        .filter($"sourceAirportId" === "3316")
        .filter($"destinationAirportId" === "3196")
        .collect
        .head
    inputRowForCheck shouldBe FlightRouteSchema(
      "MI",
      "4750",
      "SIN",
      "3316",
      "DAD",
      "3196",
      null,
      "0",
      "738 319 320"
    )

    val checkResultParsedRows =
      result
        .filter($"sourceAirportId" === "3316")
        .collect
        .map(r => r.countRoutes)
        .head
    assert(checkResultParsedRows === 15)

    val checkResultColumn = result.collect.map(r => r.sourceAirport).head
    assert(checkResultColumn === "SIN")
  }

}
