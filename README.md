# SPARKSQL RDF Benchmarking


[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3870891.svg)](https://doi.org/10.5281/zenodo.3870891)




In this Project, we present a systematic comparison of there relevant RDF relational schemas, i.e., Single Statement Table, Property Tables or Vertically-Partitioned Tables queried using Apache Spark.

We evaluate the performance Spark SQL querying engine for processing SPARQL queries using three different storage back-ends, namely, Postgres SQL, Hive, and HDFS. For the latter one, we compare four different data formats (CSV, ORC, Avro, and Parquet).
We drove our experiment using a representative query workloads from the SP2Bench benchmark scenario.

The results of our experiments show many interesting insights about the impact of the relational encoding scheme, storage backends and storage formats on the performance of the query execution process.

- You can Visist the [Project webpage](https://datasystemsgrouput.github.io/SPARKSQLRDFBenchmarking/) for more details about the datasets, benchmark, installation, preprocessing , and experiments results.

### Project Phases
-----
<p align="center"><img src="docs/images/SparkSQLRDFBenchPhases.jpg" alt="spark"> </p>

#### Phase#1
In the frst phase of our work, we presented a systematic analysis of the performance of Spark-SQL query engine (mainly
the execution time) for answering SPARQL queries over RDF repositories on a centralized single-machine. In particular, we have
performed our experiments considering: (i) alternative relational schemas for RDF, i.e., Single Statement Tables, Vertical Tables, and Property Tables; (ii) various storage backends, i.e., PostgreSQL, Hive, and HDFS, and (iii) and different data formats (e.g. CSV, Avro, Parquet, ORC). We conducted experiments on RDF datasets with (100K, 1M, and 10M) triples.

#### Phase#2
In the second phase of our project, we conducted the same settings and configurations but differently in a distributed deployments with partitioning the data. In particular, we conducted our experiments in a Spark cluster of four machines. and we worked on a larger RDF dataset of 100M dataset. Notably, we don't use PostgreSQL anymore in this phase experiments.

#### Phase#3
In this phase also we conduct the phase#2 experimetns but with way larger datsets (100M, 500M, and 1B) triples. moreover, differently from the previous phase, we apply different ranking and combined ranking criteria to quantitively and effectively help practioners to choose the best configuration combinations in such complex solution space of different dimensions (schema, partitioning, and storage).


#### Phase#4 
In this phase, we repeated the phase#2 experimetns, but this time we extended our compared relatonal experiments with new proposed relational schema representations (ExtVP, and WPT) from the State-of-the-art. Extended Vertically Partitioned Tables (ExtVP) and Wide Property Tables (WPT) are prominent optimizations that target specific workloads. Nevertheless, in a distributed context (with presenace of data pratitioning, and altenaritve storage backends) such improvements do not always outperform their baselines. Thus, we compare ExtVP with the baseline VT, and WPT with the baseline PT schema considering different partitoning techniques, and different file formats for storage.
* **Note** Phase#4 results and figures are updated and can be found in the results section [link](https://datasystemsgrouput.github.io/SPARKSQLRDFBenchmarking/OptimizedVsBaselinComparsions.html)


### Project Authors
 - [Mohamed Ragab](https://bigdata.cs.ut.ee/mohamed-ragab)
 - [Riccardo Tommasini](https://rictomm.me/)
 - [Sherif Sakr](http://kodu.ut.ee/~sakr/)
 - [Sadiq Eyvazov]() 


### Publications

Phase#1 of this project has been accepted at QuWeDa@ISWC conference in Auckland, New Zealand [PDF](http://ceur-ws.org/Vol-2496/paper5.pdf):

    Mohamed Ragab, Riccardo Tommasini and Sherif Sakr, Benchmarking SparkSQL under Alliterative RDF Relational Storage Backends, QuWeDa@ISWC 2019.

Phase#2 of this project has been accepted at Semantic Big Data (SBD 2020)@In conjunction with ACM SIGMOD 2020 in Portland, OR, USA [PDF](https://dl.acm.org/doi/10.1145/3391274.3393632):

	 Mohamed Ragab, Riccardo Tommasini, Sadiq Eyvazov and Sherif Sakr, Towards making sense of Spark-SQL performance for processing vast distributed RDF datasets, In Proceedings of The International Workshop on Semantic Big Data (SBD ’20).
	 
 <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br /><span xmlns:dct="http://purl.org/dc/terms/" property="dct:title">Spark-SQLRDF Benchmarking</span> by <a xmlns:cc="http://creativecommons.org/ns#" href="https://datasystemsgrouput.github.io/SPARKSQLRDFBenchmarking/" property="cc:attributionName" rel="cc:attributionURL">DataSystemsGroup</a> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.
