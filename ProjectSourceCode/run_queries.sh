#!/bin/bash

echo "Enter the file sizes(ex: 100M 500M 1B):"
read -a sizes

echo "Enter the file formats (ex: avro csv hive orc parquet):"
read -a file_formats

echo "Enter the class names (ex: ExtVPTables ExtVPTablesPartition WPTTables WPTTablesPartition SingleStatementTable SingleStatementTablePartition VerticalTables VerticalTablesPartition PropertyTables PropertyTablesPartition):"
read -a classes

echo "Enter the partition types (ex: Horizontal Subject Predicate):"
read -a partition

echo "Enter the number of runs(ex: 5):"
read n

for i in "${!sizes[@]}"; 
do 
      for j in "${!file_formats[@]}"; 
      do 
           for k in "${!classes[@]}"; 
	      do 
               for p in "${!partition[@]}"; 
               do
                  for (( counter=0; counter<n; counter++ ));
                  do

                     spark-submit \
                      --class "ee.ut.cs.bigdata.sp2bench.${file_formats[$j]}.${classes[$k]}" \
                      --master yarn \
                      --driver-memory 100G \
                      --executor-memory 16G \
                      --executor-cores 4 \
                      --num-executors 19 --deploy-mode client  /home/hadoop/RDFBenchMarking/ProjectSourceCode/target/scala-2.12/rdfbenchmarkingproject_2.12-0.1.jar ${sizes[$i]} ${partition[$p]}
                     
                  done
              done                     
           done
      done
done
