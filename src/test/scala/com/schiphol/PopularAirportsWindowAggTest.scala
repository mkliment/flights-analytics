package com.schiphol

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import com.schiphol.PopularAirportsWindowAgg.popularAirportsForEachWindow
import com.schiphol.storage.{FlightRouteSchema, SourceAirportWindowedCountSchema}
import com.schiphol.test.SharedSparkSession.spark
import org.apache.spark.sql.types.StringType

class PopularAirportsWindowAggTest extends AnyFunSuite with Matchers {

  // we use only 1 file and read few records on each trigger of execution of
  // stream query
  val pathToTestFile = s"src/test/resources/sample_1_*.csv"
  val topNSamples = "3"
  val triggerIntervalSeconds = 5
  val watermarkIntervalSeconds = "1"
  val windowIntervalSeconds = "10"
  val windowSlideDurSeconds = "5"
  val outputMode = "complete"

  val checkpointPath = s"data/test/out/checkpoints/$randomUUID/"
  spark.conf.set(
    "spark.sql.streaming.checkpointLocation",
    new Path(checkpointPath).toString
  )
  spark.sparkContext.setLogLevel("ERROR")

  test("should count popular airports per sliding window") {

    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._

    val testDataIter =
      TestDataHelper.getIteratorTestData(spark, pathToTestFile)
    testDataIter.nonEmpty shouldBe true

    val streamDataRoutes =
      MemoryStream[FlightRouteSchema]

    val inputDs = streamDataRoutes.toDF().as[FlightRouteSchema]

    // In order to have expected output from this test we need to have these input parameters
    topNSamples shouldBe "3"
    triggerIntervalSeconds shouldBe 5
    watermarkIntervalSeconds shouldBe "1"
    windowIntervalSeconds shouldBe "10"
    windowSlideDurSeconds shouldBe "5"
    outputMode shouldBe "complete"

    val processingStream = PopularAirportsWindowAgg
      .transform(
        spark,
        inputDs,
        watermarkIntervalSeconds,
        windowIntervalSeconds,
        windowSlideDurSeconds
      )
      .toDF()
      .writeStream
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
        val batchDs = batchDF.as[SourceAirportWindowedCountSchema]
        val result = popularAirportsForEachWindow(spark, batchDs, topNSamples)
      }
      .option("truncate", value = false)
      //      .format("console")
      .format("memory")
      .queryName("tbl_windowAgg")
      .outputMode(outputMode)
      .trigger(Trigger.ProcessingTime(triggerIntervalSeconds, TimeUnit.SECONDS))
      .start()

    val df = spark.sql("select * from tbl_windowAgg")

    // we have max 15 events in testing data
    streamDataRoutes.addData(testDataIter.slice(0, 2))
    processingStream.processAllAvailable()
//    println(
//      s"watermark is: ${processingStream.lastProgress.eventTime.get("watermark")}"
//    )
//    df.show(20, truncate = false)
    streamDataRoutes.addData(testDataIter.slice(2, 5))
    processingStream.processAllAvailable()
//    println(
//      s"watermark is: ${processingStream.lastProgress.eventTime.get("watermark")}"
//    )
//    df.show(20, truncate = false)
    streamDataRoutes.addData(testDataIter.slice(5, 15))
    processingStream.processAllAvailable()
//    println(
//      s"watermark is: ${processingStream.lastProgress.eventTime.get("watermark")}"
//    )
//    df.show(20, truncate = false)

    df.filter($"sourceAirport" === "SIN3")
      .select($"countRoutes".cast(StringType))
      .collect()
      .map { r =>
        r.getAs[String](0)
      }
      .sortWith(_ > _)
      .head shouldEqual "2"

    processingStream.stop()
  }

}
