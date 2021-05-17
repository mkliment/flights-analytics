# Flights Analytics

Note: Structure of the Spark Scala applications is taken from following repository: https://github.
com/godatadriven/scala-spark-application (thanks to author: Tim van Cann from GDD)

This repository contains simple "real-time" Spark streaming processing pipeline which produce most popular source airports per time
windows.

## Definition of processing pipeline

Processing pipeline has 2 main steps:
1. __download__: we use `com/schiphol/PopularAirportsDownloader.scala` which downloads on a local filesystem (TODO: fixme to Minio)
  `routes` dataset. (submit command with: ```./submitTask0.sh```, check steps bellow).
  from http URL. This is input dataset for the Spark jobs which are part of next step of the pipeline;
2. __process and store results__: at this step we have Spark jobs which aggregates data and provide as output top N airports
  which have the most destinations (routes) which can be reached by those source airports. For this step we have 3 different spark jobs:
* `com/schiphol/PopularAirports.scala`: a batch Spark job that read in the routes dataset. Create an overview of the top N
    airports used as source airport. Write the output to a filesystem (submit command with: ```./submitTask1.sh```).
* `com/schiphol/PopularAirportsStream.scala`: Spark streaming application same as previous Spark job using Spark Structured Streaming
   API of Spark and use the dataset file as a source (submit command with: ```./submitTask2.sh```).
* `com/schiphol/PopularAirportsWindowAgg.scala`: Spark Streaming job where aggregations are done using sliding windows. We can pick any
  window and sliding interval. The end result is the top N airports used as source airport within each window stored on filesystem
  (submit command with: ```./submitTask3.sh```).

## How to test processing pipeline

All Spark jobs are covered with 2 type of tests:
- _Integration tests_: we test read and write (I/O part of our apps) to filesystem (and in some test we test structure of the saved data)
- _Unit tests_: we test logic of transformations which we implement over input data


## How to run processing pipeline

To run the pipeline you need Docker engine on local machine.
You need to clone the repository and build docker image with added all dependencies ih the image.

Process of executing pipeline is manual by executing in sequence few commands for building Docker image and execute Spark jobs.

### Build docker image
```shell
docker build -t flights-analytics -f Dockerfile .
```
### Run docker and execute Spark jobs
```shell
# docker run in interactive mode
docker run --rm -it flights-analytics:latest
```
Inside docker container we need to executed following bash commands which submits Spark jobs:

#### Step 1: download
```shell
# submit PopularAirportsDownloader job
./opt/scripts/submitTask0.sh

# check for output data from that job on following location
ls /usr/local/data/prod/input/task0-batch/
```

#### Step 2: process and store results

##### Task 1: Batch Spark jobs - PopularAirports
```shell
# submit PopularAirports job
./opt/scripts/submitTask1.sh

# check for output data from that job on following location
ls /usr/local/data/prod/output/task1-batch/
```

##### Task 2: Stream Spark jobs - PopularAirportsStream
```shell
# submit PopularAirportsStream job
./opt/scripts/submitTask2.sh

# check for output data from that job on following location
ls /usr/local/data/prod/output/task2-stream/
```

##### Task 3: Stream with sliding window Spark jobs - PopularAirportsWindowAgg
```shell
# submit PopularAirportsWindowAgg job
./opt/scripts/submitTask3.sh

# check for output data from that job on following location
ls /usr/local/data/prod/output/task3-stream-windowing/
```
__Note:__ This Spark job should be terminated manually because is constantly waiting for new csv files to be processed after 6th (we
have 6 input files created by Task 1) micro batch.

## TODO:
- integrate this app with Minio docker



