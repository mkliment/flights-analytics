package com.schiphol

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.util.UUID.randomUUID

import com.schiphol.Functions.selectSchema
import com.schiphol.storage.{FlightRouteSchema, Storage}
import com.schiphol.test.SharedSparkSession.spark

class PopularAirportsStreamTestIT extends AnyFunSuite with Matchers with Logging {

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
    private def routesDfColumnsMapper(df: DataFrame) = {
      df.as[FlightRouteSchema]
    }
    override def remoteFlightRoutes: Dataset[FlightRouteSchema] = {
      routesDfColumnsMapper(spark.read.csv("not_used_in_this_test"))
    }

    override def flightRoutes: Dataset[FlightRouteSchema] = {
      readCsv("not_used_in_this_test", "true").as[FlightRouteSchema]
    }

    override def writeStringToFile(outputPath: String, outputFile: String, content: String, append: Boolean = true): Unit = {
      println(s"not_used_in_this_test")
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

    override def writeToCsv(ds: Dataset[_], saveMode: String, location: String): Unit = {
      ds.write
        .option("header", "true")
        .mode("overwrite")
        .csv("data/test/out/outStream")
    }

  }

  test(
    "stream apps using Storage should read stream from disk and write to disk"
  ) {
    import spark.implicits._
    val config = UsageConfig(Some("2"))
    PopularAirportsStream.run(spark, config, new LocalStorage(spark))
  }

}
