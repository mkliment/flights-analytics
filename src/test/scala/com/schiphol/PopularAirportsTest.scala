package com.schiphol

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.schiphol.test.SharedSparkSession.spark
import com.schiphol.storage.FlightRouteSchema

class PopularAirportsTest extends AnyFunSuite with Matchers {

  import spark.implicits._

  test(
    "transform should count number of routes/destinations per Source Airport"
  ) {
    val dsCnt = spark.createDataset(
      Seq(
        FlightRouteSchema(
          "BA",
          "1355",
          "SIN",
          "3316",
          "LHR",
          "507",
          "\\N",
          "0",
          "744 777"
        ),
        FlightRouteSchema(
          "BA",
          "1355",
          "SIN",
          "3316",
          "MEL",
          "3339",
          "Y",
          "0",
          "744"
        ),
        FlightRouteSchema(
          "TOM",
          "5013",
          "ACE",
          "1055",
          "BFS",
          "465",
          "\\N",
          "0",
          "320"
        )
      )
    )

    val result = PopularAirports
      .transform(spark, dsCnt, "10")

    val cnt = result
      .filter($"sourceAirportId" === "3316")
      .collect
      .map(r => r.countRoutes)
      .head
    assert(cnt === 2)
  }

  test("transform should output topN popular routes per Source Airport") {
    val ds = spark.createDataset(
      Seq(
        FlightRouteSchema(
          "BA",
          "1355",
          "SIN1",
          "1",
          "LHR",
          "507",
          "\\N",
          "0",
          "744 777"
        ),
        FlightRouteSchema(
          "BA",
          "1355",
          "SIN1",
          "1",
          "MEL",
          "3339",
          "Y",
          "0",
          "744"
        ),
        FlightRouteSchema(
          "BA",
          "1355",
          "SIN2",
          "2",
          "MEL",
          "3339",
          "Y",
          "0",
          "744"
        ),
        FlightRouteSchema(
          "BA",
          "1355",
          "SIN3",
          "3",
          "MEL",
          "3339",
          "Y",
          "0",
          "744"
        ),
        FlightRouteSchema(
          "TOM",
          "5013",
          "SIN4",
          "4",
          "BFS",
          "465",
          "\\N",
          "0",
          "320"
        )
      )
    )

    val result = PopularAirports
      .transform(spark, ds, "2")
    assert(result.count === 2)

    val lastRowLength = result.filter($"sourceAirportId" === "4").collect.length
    assert(lastRowLength === 0)
  }

}
