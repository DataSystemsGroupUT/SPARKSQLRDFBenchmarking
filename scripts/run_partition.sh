#!/bin/bash

SCHEMA=ExtVPTablesPartition
FORMAT=parquet
DS=250M
PARTITION=horizontal # subject | horizontal | predicate

RDF_BENCHMARK_CLASS="ee.ut.cs.bigdata.sp2bench.$FORMAT.$SCHEMA"
RDF_BENCHMARK_JAR="/home/hadoop/RDFBenchMarking/ProjectSourceCode/target/scala-2.12/rdfbenchmarkingproject_2.12-0.1.jar"

spark-submit \
  --class $RDF_BENCHMARK_CLASS \
  --master "spark://172.17.77.48:7077" \
  --driver-memory 100G \
  --executor-memory 16G \
  --executor-cores 4 \
  --num-executors 19 \
  --deploy-mode client \
  $RDF_BENCHMARK_JAR $DS $PARTITION