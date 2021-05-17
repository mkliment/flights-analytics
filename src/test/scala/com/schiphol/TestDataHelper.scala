package com.schiphol

import com.schiphol.storage.FlightRouteSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object TestDataHelper {

  val LOG: Logger = LoggerFactory.getLogger("TestDataHelper")

  def getIteratorTestData(spark: SparkSession, pathToTestFiles: String): Iterator[FlightRouteSchema] = {
    import spark.implicits._

    val testDataIterator = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(s"$pathToTestFiles")
      .select(
        FlightRouteSchema.columnsMap.keys.toList
          .map(c => col(c).as(FlightRouteSchema.columnsMap.getOrElse(c, c))): _*
      )
      .as[FlightRouteSchema]
      .collect()
      .toIterator

    LOG.info(s"Created iterator for test data for input files on ${pathToTestFiles}")

    testDataIterator
  }

}
