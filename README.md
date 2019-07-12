# SPARKSQLRDFBenchmarking
In this Project, we present a systematic comparison of there relevant RDF relational schemas, i.e., Single Statement Table, Property Tables or Vertically-Partitioned Tables queried using Apache Spark.
We evaluate the performance Spark SQL querying engine for processing SPARQL queries using three different storage back-ends, namely, Postgres SQL, Hive, and HDFS. For the latter one, we compare four different data formats (CSV, ORC, Avro, and Parquet).
We drove our experiment using a representative query workloads from the \texttt{SP2Bench} benchmark scenario.
The results of our experiments show many interesting insights about the impact of the relational encoding scheme, storage backends and storage formats on the performance of the query execution process.
