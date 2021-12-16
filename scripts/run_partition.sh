#!/bin/bash

BENCHMARK=sp2bench
SCHEMA=WPTTablesPartition
FORMATS=(parquet avro csv orc)
DS=100M
PARTITION=Predicate # Subject | Horizontal | Predicate

RDF_BENCHMARK_JAR="/home/hadoop/SPARKSQLRDFBenchmarking/ProjectSourceCode/target/scala-2.12/rdfbenchmarkingproject_2.12-0.1.jar"

for FORMAT in ${FORMATS[@]}; do

  RDF_BENCHMARK_CLASS="ee.ut.cs.bigdata.$BENCHMARK.$FORMAT.$SCHEMA"
  
  spark-submit \
    --class $RDF_BENCHMARK_CLASS \
    --master "yarn" \
    --driver-memory 100G \
    --executor-memory 16G \
    --executor-cores 4 \
    --num-executors 19 \
    --deploy-mode client \
    $RDF_BENCHMARK_JAR $DS $PARTITION
  done
