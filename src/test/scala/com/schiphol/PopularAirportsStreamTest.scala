package com.schiphol

import com.schiphol.storage.{FlightRouteSchema, SourceAirportWindowedCountSchema}
import com.schiphol.test.SharedSparkSession.spark
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit

class PopularAirportsStreamTest extends AnyFunSuite with Matchers {

  // we use only 1 file and read few records on each trigger of execution of
  // stream query
  val pathToTestFile = s"src/test/resources/sample_1_*.csv"
  val topNSamples = "3"
  val outputMode = "complete"

  val checkpointPath = s"data/test/out/checkpoints/$randomUUID/"
  spark.conf.set(
    "spark.sql.streaming.checkpointLocation",
    new Path(checkpointPath).toString
  )
  spark.sparkContext.setLogLevel("ERROR")

  test("should count popular airports per window") {

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
    outputMode shouldBe "complete"

    // Trigger.Once() is not suitable in the unit test because we will lose the state of query in memory and can't check the result of
    // the test. To simulate same behaviour as in the main spark job we will use ProcessingTime but load all records at once and trigger
    // only once the stream query with processingStream.processAllAvailable()
    val processingStream = PopularAirportsStream
      .transform(
        spark,
        inputDs,
        topNSamples
      )
      .toDF()
      .writeStream
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
      }
      .option("truncate", value = false)
      //      .format("console")
      .format("memory")
      .queryName("tbl_stream")
      .outputMode(outputMode)
      .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
      .start()

    val df = spark.sql("select * from tbl_stream")

    // we have max 15 events in testing data, let's use all of them at once
    streamDataRoutes.addData(testDataIter.slice(0, 20))
    processingStream.processAllAvailable()

    df.filter($"sourceAirport" === "SIN3")
      .select($"countRoutes".cast(StringType))
      .collect()
      .map { r =>
        r.getAs[String](0)
      }
      .head shouldEqual "2"

    processingStream.stop()
  }

}
