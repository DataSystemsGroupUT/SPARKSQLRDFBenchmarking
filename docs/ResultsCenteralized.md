### Centeralized Experiments
#### Hardware and Software Configurations

Our experiments have been executed on a Desktop PC running a Cloudera Virtual Machine (VM) v.5.13 with Centos v7.3 Linux system, running on Intel(R) Core(TM) i5-8250U 1.60 GHz X64-based CPU and 24 GB DDR3 of physical memory. We also used a 64GB virtual hard drive for our VM. We used Spark V2.3 parcel on Cloudera VM to fully support Spark-SQL capabilities. We used the already installed Hive service on Cloudera VM (version:hive-1.1.0+cdh5.16.1+1431). We have installed a relational DB PostgreSQL (V. 11.4).

*100K Results
<img src="figures/centeralizedExperiments/PT100K.png" alt="spark" width="250" height="150">       <img src="https://github.com/EyvazovSadiq/SPARKSQLRDFBenchmarking/blob/master/figures/Partitioning_100M_VT.png" alt="spark" width="250" height="150">       <img src="https://github.com/EyvazovSadiq/SPARKSQLRDFBenchmarking/blob/master/figures/Partitioning_100M_PT.png" alt="spark" width="250" height="150">


  * Relational Schema


<img src="https://github.com/EyvazovSadiq/SPARKSQLRDFBenchmarking/blob/master/figures/Schema_100M_HP.png" alt="spark" width="250" height="150">       <img src="https://github.com/EyvazovSadiq/SPARKSQLRDFBenchmarking/blob/master/figures/Schema_100M_SBP.png" alt="spark" width="250" height="150">       <img src="https://github.com/EyvazovSadiq/SPARKSQLRDFBenchmarking/blob/master/figures/Schema_100M_PBP.png" alt="spark" width="250" height="150">


  * Storage Format
  
<img src="https://github.com/EyvazovSadiq/SPARKSQLRDFBenchmarking/blob/master/figures/StorageFormats_100M_ST.png" alt="spark" width="250" height="150">       <img src="https://github.com/EyvazovSadiq/SPARKSQLRDFBenchmarking/blob/master/figures/StorageFormats_100M_VT.png" alt="spark" width="250" height="150">       <img src="https://github.com/EyvazovSadiq/SPARKSQLRDFBenchmarking/blob/master/figures/StorageFormats_100M_PT.png" alt="spark" width="250" height="150">
