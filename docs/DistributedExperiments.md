## Distributed Experiments

**Hardware and Software Configurations**: Our experiments have been executed on a bare metal cluster of four machines with a CentOS-Linux V7 OS, running on a 32-AMD cores per node processors, and 128 GB of memory per node, alongside with a high speed 2 TB SSD drive as the data drive on each node. We used Spark V2.4 to fully support Spark-SQL capabilities. We used Hive V3.2.1. In particular, our Spark cluster is consisted of one master node and three worker machines, while Yarn is used as the resource manager, which in total uses 330 GB and 84 virtual processing cores.


### Execution Runtimes

#### 100M Triples Dataset Results

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



#### 500M Triples Dataset Results

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
