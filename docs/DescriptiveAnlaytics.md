### Descriptive and Diagnostic Analysis (SP2Bench Scenario)

#### Query Performance Analysis

SP2Bench Querie Averge runtime figures can be seen in this [link](https://datasystemsgrouput.github.io/SPARKSQLRDFBenchmarking/DistributedExperiments_Long_Short_RunningTime_Queries.html).

##### Short , medium, and long running queries:
 
From the figures, we can observe that queries **Q1**,**Q3**,**Q10**, and **Q11** are the least impactful queries (i.e., have the lowest running times). Thus, we call these queries short-running queries. On the other hand, queries **Q2**,**Q4**,  and **Q8** have the longest runtimes. The remaining queries **Q5**, **Q6** ,**Q7**,and **Q9** are medium-running queries. 

In the following, we focus our analysis on the longest running queries, as they may hide interesting  insights about the approach limitations.

- Query Q2 shows a low average execution time when using the ST schema or the VT schema. However, for the PT schema, it has much higher runtimes (at some cases 2X of runtime).
- This observation is confirmed in the 100M, 250M, and 500M datasets, and despite the partitioning technique of choice. 
  - Therefore, we can conclude that PT schema for Query Q2 is not the best option to choose. 
- The previous observation is only valid with neglecting the bad performance of the CSV and Avro in the ST schema. However, it gives a clear answer of the impact of storage impact on our observations.


- Query Q4 has the highest latency for all the relational schema, for our different partitioning techniques, and for the different storage backends. 
- Query Q8 immediately follows, as the second longest running query for the different partitioning techniques, and for the different storage backends. 
  - Interestingly, Q8 shows significant enhancement when using the PT schema. 
  -  We can observe that in the 100M dataset figures, and it is even clearer by scaling up to 250M and 500M dataset.
  
- Finally, we can also notice that, the ST relational schema has the worst impact on the majority of the queries. 
- While, VT schema is mostly the best performing one, directly followed by the PT schema. 
- Regarding the storage backends, we generally observe that the columnar file formats of HDFS (ORC, and Parquet) are the best performing, followed by Hive. 
- Whereas, the row-oriented Avro and the CSV textual file format of HDFS are mostly the worst performing backends. 
- Regarding to the partitioning impact, the SBP approach tends to considerably outperform its other opponents. 
- Particularly, it directly outperforms HP, leaving the PBP technique in the worst rank.
- However, we cannot straightforwardly state that the VT schema is outperforming the PT schema.
  - As it has been shown in Q8, the PT schema is obviously outperforming the VT schema. 
- Similarly, we cannot state that Avro file format is always the worst or the second worst performing storage backend (however it is in the majority of queries), as Avro has been the best performing storage backend several times.

