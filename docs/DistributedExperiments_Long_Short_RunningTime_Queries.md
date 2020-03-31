## Distributed Experiments

**Hardware and Software Configurations**: Our experiments have been executed on a bare metal cluster of four machines with a CentOS-Linux V7 OS, running on a 32-AMD cores per node processors, and 128 GB of memory per node, alongside with a high speed 2 TB SSD drive as the data drive on each node. We used Spark V2.4 to fully support Spark-SQL capabilities. We used Hive V3.2.1. In particular, our Spark cluster is consisted of one master node and three worker machines, while Yarn is used as the resource manager, which in total uses 330 GB and 84 virtual processing cores.

- In the following figures we cateogrized running times queries into Long-Running Queries (Q1,Q3, Q10, Q11), and Short-Running Queries. We think that this grouping makes the queris plots looks clearer than the grouped query runtimes plots, due to they have big different running times.

### Execution Runtimes (100M Triples Dataset Results)


#### **100M Triples Dataset Results Figures**

* **[Horizontally Partitioned]**
- Single Table Schema (ST) Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Horizontal-Short/ST-100M-Horizontal Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Horizontal-Long/ST-100M-Horizontal Long queries.png" alt="spark" > 

- Vertical Tables Schema (VT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Horizontal-Short/VT-100M-Horizontal Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Horizontal-Long/VT-100M-Horizontal Long queries.png" alt="spark" > 

- Property Tables Schema (PT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Horizontal-Short/PT-100M-Horizontal Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Horizontal-Long/PT-100M-Horizontal Long queries.png" alt="spark" > 


* **[Predicate-Based Partitioned]**
- Single Table Schema (ST) Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Predicate-Short/ST-100M-Predicate Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Predicate-Long/ST-100M-Predicate Long queries.png" alt="spark" >

- Vertical Tables Schema (VT), Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Predicate-Short/VT-100M-Predicate Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Predicate-Long/VT-100M-Predicate Long queries.png" alt="spark" >

- Property Tables Schema (PT), Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Predicate-Short/PT-100M-Predicate Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Predicate-Long/PT-100M-Predicate Long queries.png" alt="spark" >


* **[Subject-Based Partitioned]**
- Single Table Schema (ST) Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Subject-Short/ST-100M-Subject Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Subject-Long/ST-100M-Subject Long queries.png" alt="spark" >

- Vertical Tables Schema (VT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Subject-Short/VT-100M-Subject Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Subject-Long/VT-100M-Subject Long queries.png" alt="spark" > 

- Property Tables Schema (PT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Subject-Short/PT-100M-Subject Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/100M/100M-Subject-Long/PT-100M-Subject Long queries.png" alt="spark" > 



#### **250M Triples Dataset Results Figures**

* **[Horizontally Partitioned]**
- Single Table Schema (ST) Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Horizontal-Short/ST-250M-Horizontal Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Horizontal-Long/ST-250M-Horizontal Long queries.png" alt="spark" > 

- Vertical Tables Schema (VT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Horizontal-Short/VT-250M-Horizontal Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Horizontal-Long/VT-250M-Horizontal Long queries.png" alt="spark" > 

- Property Tables Schema (PT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Horizontal-Short/PT-250M-Horizontal Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Horizontal-Long/PT-250M-Horizontal Long queries.png" alt="spark" > 


* **[Predicate-Based Partitioned]**
- Single Table Schema (ST) Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Predicate-Short/ST-250M-Predicate Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Predicate-Long/ST-250M-Predicate Long queries.png" alt="spark" >

- Vertical Tables Schema (VT), Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Predicate-Short/VT-250M-Predicate Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Predicate-Long/VT-250M-Predicate Long queries.png" alt="spark" >

- Property Tables Schema (PT), Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Predicate-Short/PT-250M-Predicate Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Predicate-Long/PT-250M-Predicate Long queries.png" alt="spark" >


* **[Subject-Based Partitioned]**
- Single Table Schema (ST) Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Subject-Short/ST-250M-Subject Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Subject-Long/ST-250M-Subject Long queries.png" alt="spark" >

- Vertical Tables Schema (VT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Subject-Short/VT-250M-Subject Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Subject-Long/VT-250M-Subject Long queries.png" alt="spark" > 

- Property Tables Schema (PT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Subject-Short/PT-250M-Subject Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/250M/250M-Subject-Long/PT-250M-Subject Long queries.png" alt="spark" > 



#### **500M Triples Dataset Results Figures**

* **[Horizontally Partitioned]**
- Single Table Schema (ST) Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Horizontal-Short/ST-500M-Horizontal Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Horizontal-Long/ST-500M-Horizontal Long queries.png" alt="spark" > 

- Vertical Tables Schema (VT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Horizontal-Short/VT-500M-Horizontal Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Horizontal-Long/VT-500M-Horizontal Long queries.png" alt="spark" > 

- Property Tables Schema (PT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Horizontal-Short/PT-500M-Horizontal Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Horizontal-Long/PT-500M-Horizontal Long queries.png" alt="spark" > 


* **[Predicate-Based Partitioned]**
- Single Table Schema (ST) Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Predicate-Short/ST-500M-Predicate Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Predicate-Long/ST-500M-Predicate Long queries.png" alt="spark" >

- Vertical Tables Schema (VT), Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Predicate-Short/VT-500M-Predicate Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Predicate-Long/VT-500M-Predicate Long queries.png" alt="spark" >

- Property Tables Schema (PT), Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Predicate-Short/PT-500M-Predicate Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Predicate-Long/PT-500M-Predicate Long queries.png" alt="spark" >


* **[Subject-Based Partitioned]**
- Single Table Schema (ST) Short(left) and Long (right) Queries:

 <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Subject-Short/ST-500M-Subject Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Subject-Long/ST-500M-Subject Long queries.png" alt="spark" >

- Vertical Tables Schema (VT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Subject-Short/VT-500M-Subject Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Subject-Long/VT-500M-Subject Long queries.png" alt="spark" > 

- Property Tables Schema (PT), Short(left) and Long (right) Queries:

<img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Subject-Short/PT-500M-Subject Short queries.png" alt="spark" > <img src="figures/DistributedExperiments/ExecutionRuntimes/500M/500M-Subject-Long/PT-500M-Subject Long queries.png" alt="spark" > 


