package com.schiphol.storage

import java.io.{FileWriter, PrintWriter}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkFiles

import java.nio.file.{Files, Paths}
import com.schiphol.Functions.{selectSchema, selectSortedSchema}
import org.slf4j.{Logger, LoggerFactory}

trait Storage {

  def remoteFlightRoutes: Dataset[FlightRouteSchema]
  def flightRoutes: Dataset[FlightRouteSchema]
  def flightRoutesStream: Dataset[FlightRouteSchema]

  def writeToCsv(ds: Dataset[_], saveMode: String, location: String)
  def writeStringToFile(outputPath: String, outputFile: String, content: String, append: Boolean = true)
}

class LocalStorage(spark: SparkSession) extends Storage {

  /**
    * Storage defines all input and output logic. How and where tables and files
    * should be read and saved.
    */
  import spark.implicits._
  val LOG: Logger = LoggerFactory.getLogger("Storage")

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
    // TODO: this is hack, download file on each executor, should be changed
    spark.sparkContext.addFile(
      "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
    )
    val remoteFlightRoutesDf =
      readCsv(SparkFiles.get("routes.dat"), "false")

    routesDfColumnsMapper(remoteFlightRoutesDf)
  }

  override def flightRoutes: Dataset[FlightRouteSchema] = {
    val flightRoutesDf =
      readCsv("/usr/local/data/prod/input/task0-batch/*.csv", "true")
    selectSchema[FlightRouteSchema](flightRoutesDf)
      .as[FlightRouteSchema]
  }

  private def readCsvStream(location: String, schema: StructType, header: String) = {
    // default encoding of API is is UTF-8, same as our dataset
    spark.readStream
      .option("header", header)
      .option("inferSchema", "true")
      .option("delimiter", ",")
      // don't use `/` for escape character as we have `/N` values in our dataset
      .option("maxFilesPerTrigger", 1)
      .schema(schema)
      .csv(location)
  }

  override def flightRoutesStream: Dataset[FlightRouteSchema] = {
    import spark.implicits._
    val schema = Encoders.product[FlightRouteSchema].schema

    val flightRoutesStreamDf =
      readCsvStream("/usr/local/data/prod/input/task0-batch/*.csv", schema, "true")

    val flightRouteStreamDs = selectSchema[FlightRouteSchema](flightRoutesStreamDf)
      .as[FlightRouteSchema]
    flightRouteStreamDs
  }

  override def writeToCsv(ds: Dataset[_], saveMode: String, location: String): Unit = {
    ds.write
      .option("header", "true")
      .mode(saveMode)
      .csv(location)
  }

  override def writeStringToFile(outputPath: String, outputFile: String, content: String, append: Boolean = true): Unit = {
    val fullPath = s"$outputPath/$outputFile"
    val dir = Paths.get(outputPath)

    if (!Files.exists(dir)) {
      Files.createDirectories(dir)
      LOG.info(s"Created missing directory $dir")
    }

    val fileWriter = new FileWriter(fullPath, append)
    val printWriter = new PrintWriter(fileWriter)

    printWriter.println(content)
    fileWriter.close()
  }

}
