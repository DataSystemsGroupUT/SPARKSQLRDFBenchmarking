## Reproducibility of Optimized SPARQL Query Execution Using Spark-SQL Experiements (Distributed)

**Hardware and Software Configurations**: Our experiments have been executed on a bare metal cluster of four machines with a CentOS-Linux V7 OS, running on a 32-AMD cores per node processors, and 128 GB of memory per node, alongside with a high speed 2 TB SSD drive as the data drive on each node. We used Spark V2.4 to fully support Spark-SQL capabilities. We used Hive V3.2.1. In particular, our Spark cluster is consisted of one master node and three worker machines, while Yarn is used as the resource manager, which in total uses 330 GB and 84 virtual processing cores.

**<font color="red">Experiments</font>**: we investigate systematically the pitfalls behind implementing these optimizations over SparkSQL. We compare ExtVP with VT and WPT with PT considering (1) three different partitioning techniques, i.e., Horizontal, Subject-based, and Predicate-based partitioning and (2) five different storage formats, i.e., ORC, CSV, Parquet, Avro, and Hive.

* **Note**:  - In the following figures we cateogrized running times queries into Long-Running Queries (Q1,Q3, Q10, Q11), and Short-Running Queries. We think that this grouping makes the queris plots looks clearer than the grouped query runtimes plots, due to they have big different running times.



### Execution Runtimes (100M Triples Dataset Results)

* **100M Results[Horizontally Partitioned]**

* Avro Short
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Horizontal/avro-100M-Horizontal Short queries.png" alt="spark" >
</p> 

* Avro Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Horizontal/avro-100M-Horizontal Long queries.png" alt="spark" >
</p>

* CSV Short
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Horizontal/csv-100M-Horizontal Short queries.png" alt="spark" >
</p>

* CSV Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Horizontal/csv-100M-Horizontal Long queries.png" alt="spark" >
</p>

* Hive Short
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Horizontal/hive-100M-Horizontal Short queries.png" alt="spark" >
</p>

* Hive Long

<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Horizontal/hive-100M-Horizontal Long queries.png" alt="spark" >
</p>

* ORC Short 
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Horizontal/orc-100M-Horizontal Short queries.png" alt="spark" >
</p>

* ORC Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Horizontal/orc-100M-Horizontal Long queries.png" alt="spark" >
</p>

* Parquet Short 
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Horizontal/parquet-100M-Horizontal Short queries.png" alt="spark" >
</p>

* Parquet Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Horizontal/parquet-100M-Horizontal Long queries.png" alt="spark" >
</p>



* **100M Results[Predicate-Based Partitioned]**

* Avro Short 
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Predicate/avro-100M-Predicate Short queries.png" alt="spark" >
</p>

* Avro Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Predicate/avro-100M-Predicate Long queries.png" alt="spark" >
</p>

* CSV Short
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Predicate/csv-100M-Predicate Short queries.png" alt="spark" >
</p>

* CSV Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Predicate/csv-100M-Predicate Long queries.png" alt="spark" >
</p>

* Hive Short
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Predicate/hive-100M-Predicate Short queries.png" alt="spark" >
</p>

* Hive Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Predicate/hive-100M-Predicate Long queries.png" alt="spark" >
</p>

* ORC Short 
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Predicate/orc-100M-Predicate Short queries.png" alt="spark" >
</p>

* ORC Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Predicate/orc-100M-Predicate Long queries.png" alt="spark" >
</p>

* Parquet Short 
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Predicate/parquet-100M-Predicate Short queries.png" alt="spark" >
</p>

* Parquet Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Predicate/parquet-100M-Predicate Long queries.png" alt="spark" >
</p>

* **100M Results[Subject-Based Partitioned]**

* Avro Short 
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Subject/avro-100M-Subject Short queries.png" alt="spark" >
</p>

* Avro Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Subject/avro-100M-Subject Long queries.png" alt="spark" >
</p>

* CSV Short
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Subject/csv-100M-Subject Short queries.png" alt="spark" >
</p>

* CSV Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Subject/csv-100M-Subject Long queries.png" alt="spark" >
</p>

* Hive Short
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Subject/hive-100M-Subject Short queries.png" alt="spark" >
</p>

* Hive Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Subject/hive-100M-Subject Long queries.png" alt="spark" >
</p>

* ORC Short 
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Subject/orc-100M-Subject Short queries.png" alt="spark" >
</p>

* ORC Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Subject/orc-100M-Subject Long queries.png" alt="spark" >
</p>

* Parquet Short 
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Subject/parquet-100M-Subject Short queries.png" alt="spark" >
</p>
* Parquet Long
<p align="center">
<img src="figures/DistributedExperiments/OptimizedVSBaselineSchemata/plots_short_long/100M-Subject/parquet-100M-Subject Long queries.png" alt="spark" >
</p>
