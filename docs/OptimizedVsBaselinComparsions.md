## Reproducibility of Optimized SPARQL Query Execution Using Spark-SQL Experiements (Distributed)

**Hardware and Software Configurations**: Our experiments have been executed on a bare metal cluster of four machines with a CentOS-Linux V7 OS, running on a 32-AMD cores per node processors, and 128 GB of memory per node, alongside with a high speed 2 TB SSD drive as the data drive on each node. We used Spark V2.4 to fully support Spark-SQL capabilities. We used Hive V3.2.1. In particular, our Spark cluster is consisted of one master node and three worker machines, while Yarn is used as the resource manager, which in total uses 330 GB and 84 virtual processing cores.

**Experiments: we investigate systematically the pitfalls behind implementing these optimizations over SparkSQL. We compare ExtVP with VT and WPT with PT considering (1) three different partitioning techniques, i.e., Horizontal, Subject-based, and Predicate-based partitioning and (2) five different storage formats, i.e., ORC, CSV, Parquet, Avro, and Hive.

* **Note**:  We also provide Long and Short Running Query Results Figures that looks clearer [here](DistributedExperiments_Long_Short_RunningTime_Queries.md)


### Execution Runtimes (100M Triples Dataset Results)

* **100M Results[Horizontally Partitioned]**

<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Horizontal/ST-100M-Horizontal All queries.png" alt="spark" > 
<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Horizontal/VT-100M-Horizontal All queries.png" alt="spark" >
<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Horizontal/PT-100M-Horizontal All queries.png" alt="spark" >

* **100M Results[Predicate-Based Partitioned]**

<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Predicate/ST-100M-Predicate All queries.png" alt="spark" > 
<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Predicate/VT-100M-Predicate All queries.png" alt="spark" >
<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Predicate/PT-100M-Predicate All queries.png" alt="spark" >

* **100M Results[Subject-Based Partitioned]**

<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Subject/ST-100M-Subject All queries.png" alt="spark" > 
<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Subject/VT-100M-Subject All queries.png" alt="spark" >
<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Subject/PT-100M-Subject All queries.png" alt="spark" >


### Execution Runtimes (250M Triples Dataset Results)

* **250M Results[Horizontally Partitioned]**

<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Horizontal/ST-250M-Horizontal All queries.png" alt="spark" > 
<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Horizontal/VT-250M-Horizontal All queries.png" alt="spark" >
<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Horizontal/PT-250M-Horizontal All queries.png" alt="spark" >

* **250M Results[Predicate-Based Partitioned]**

<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Predicate/ST-250M-Predicate All queries.png" alt="spark" > 
<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Predicate/VT-250M-Predicate All queries.png" alt="spark" >
<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Predicate/PT-250M-Predicate All queries.png" alt="spark" >

* **250M Results[Subject-Based Partitioned]**

<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Subject/ST-250M-Subject All queries.png" alt="spark" > 
<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Subject/VT-250M-Subject All queries.png" alt="spark" >
<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Subject/PT-250M-Subject All queries.png" alt="spark" >





### Execution Runtimes (500M Triples Dataset Results)

* **500M Results[Horizontally Partitioned]**

<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Horizontal/ST-500M-Horizontal All queries.png" alt="spark" > 
<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Horizontal/VT-500M-Horizontal All queries.png" alt="spark" >
<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Horizontal/PT-500M-Horizontal All queries.png" alt="spark" >

* **500M Results[Predicate-Based Partitioned]**

<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Predicate/ST-500M-Predicate All queries.png" alt="spark" > 
<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Predicate/VT-500M-Predicate All queries.png" alt="spark" >
<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Predicate/PT-500M-Predicate All queries.png" alt="spark" >

* **500M Results[Subject-Based Partitioned]**

<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Subject/ST-500M-Subject All queries.png" alt="spark" > 
<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Subject/VT-500M-Subject All queries.png" alt="spark" >
<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Subject/PT-500M-Subject All queries.png" alt="spark" >
