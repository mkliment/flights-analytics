package com.schiphol.storage

trait Schema

object FlightRouteSchema {

  def columnsMap: Map[String, String] =
    Map(
      "_c0" -> "airline",
      "_c1" -> "airlineId",
      "_c2" -> "sourceAirport",
      "_c3" -> "sourceAirportId",
      "_c4" -> "destinationAirport",
      "_c5" -> "destinationAirportId",
      "_c6" -> "codeShare",
      "_c7" -> "stops",
      "_c8" -> "equipment"
    )
}

case class FlightRouteSchema(
    airline: String,
    airlineId: String,
    sourceAirport: String,
    sourceAirportId: String,
    destinationAirport: String,
    destinationAirportId: String,
    codeShare: String,
    stops: String,
    equipment: String)
  extends Schema

// PopularAirportsDownloader is creating csv with inverted order of columns, if we don't don't use following case class schema
// TODO: find why and fixme
case class FlightRouteInvertedSchema(
    equipment: String,
    stops: String,
    codeShare: String,
    destinationAirportId: String,
    destinationAirport: String,
    sourceAirportId: String,
    sourceAirport: String,
    airlineId: String,
    airline: String)
  extends Schema

case class SourceAirportCountSchema(sourceAirport: String, sourceAirportId: String, countRoutes: Long) extends Schema

// we could use this class to add in SourceAirportWindowedCountSchema, but I prefer more flat structure
//case class WindowSchema(start: TimestampType, end: TimestampType) extends Schema

case class SourceAirportWindowedCountSchema(
    sourceAirport: String,
    sourceAirportId: String,
    windowStart: String,
    windowEnd: String,
    countRoutes: Long)
  extends Schema
