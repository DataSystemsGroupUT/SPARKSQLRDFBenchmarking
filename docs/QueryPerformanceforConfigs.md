### Query Performance for Configuration Combination

The following figure quantitatively shows the impact of certain query over the selected confguration combination. We can notice sevral interesting points:
- Q1, Q3, Q5 are the least impacting queries (take the minimum running times across all the configuration combinations).
- Q4, followed by Q8, then Q2 are the highes impactful queries across all the configuration cominations. 

<img src="figures/DistributedExperiments/ConfigurationsQuerExecutionPerformance/queryconfigs.png" alt="spark" > 






The following figures show the best and worst confguration combinations for running the SP2Bench 11 queries. we run our experiments 5 times, and we take the average run time. 

* 100M Results

Notably, Figures of Q7 are missing since the query failed some times, while the results of Q9 bcause it is not implemented in the third schema (PT).

**Q1** 
<img src="figures/DistributedExperiments/ConfigurationsQuerExecutionPerformance/Q1.JPG" alt="spark" > 

**Q2** 
<img src="figures/DistributedExperiments/ConfigurationsQuerExecutionPerformance/Q2.JPG" alt="spark" > 

**Q3** 
<img src="figures/DistributedExperiments/ConfigurationsQuerExecutionPerformance/Q3.JPG" alt="spark" > 

**Q4** 
<img src="figures/DistributedExperiments/ConfigurationsQuerExecutionPerformance/Q4.JPG" alt="spark" > 

**Q5** 
<img src="figures/DistributedExperiments/ConfigurationsQuerExecutionPerformance/Q5.JPG" alt="spark" > 

**Q6** 
<img src="figures/DistributedExperiments/ConfigurationsQuerExecutionPerformance/Q6.JPG" alt="spark" > 

**Q8** 
<img src="figures/DistributedExperiments/ConfigurationsQuerExecutionPerformance/Q8.JPG" alt="spark" > 

**Q10** 
<img src="figures/DistributedExperiments/ConfigurationsQuerExecutionPerformance/Q10.JPG" alt="spark" > 

**Q11** 
<img src="figures/DistributedExperiments/ConfigurationsQuerExecutionPerformance/Q11.JPG" alt="spark" > 
