package com.schiphol

import com.schiphol.storage._
import org.apache.spark.sql.{Dataset, SparkSession}

import com.schiphol.Functions.selectSchema

object PopularAirportsDownloader extends SparkJob {

  override def appName: String = "PopularAirportsDownloader"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {

    /**
      * This function in all spark jobs only does I/O, no logic.
      * These function is check by IT tests.
      */
    // input data
    val remoteRoutes = storage.remoteFlightRoutes

    // output transformed data
    val result = transform(spark, remoteRoutes)

    // persist output data
    // better solution is parquet format for next steps in the pipeline, faster download of data and less storage consumption
    // but to be able easy to check the results lets use CSV
    storage.writeToCsv(result, "overwrite", s"/usr/local/data/prod/input/task0-batch")

  }

  def transform(spark: SparkSession, routes: Dataset[FlightRouteSchema]): Dataset[FlightRouteInvertedSchema] = {

    /**
      * Any logic is implemented in transform function in all Spark jobs.
      * Unit tests are checking these functions.
      */
    import spark.implicits._

    // to driver, small file, to enforce repartition
    val localFR = routes
      .collect()
      .map { r =>
        FlightRouteSchema(
          r.airline,
          r.airlineId,
          r.sourceAirport,
          r.sourceAirportId,
          r.destinationAirport,
          r.destinationAirportId,
          r.codeShare,
          r.stops,
          r.equipment
        )
      }

    val outDf =
      localFR.toSeq.toDF()

    // TODO: we used inverted schema because that's how downloader is saving, fixme
    val outDs = selectSchema[FlightRouteInvertedSchema](outDf)
      .repartition(6)
      .as[FlightRouteInvertedSchema]

    outDs
  }

}
