#!/usr/bin/env bash

/opt/spark/bin/spark-submit \
--class com.schiphol.PopularAirportsStream \
--master local[4] \
/opt/spark/flights-analytics-assembly-0.0.1-SNAPSHOT.jar
