### Results
-----
  * Centralized Expeiments results (single machine, smaller datsets)
    * [Centralized Experiments](ResultsCenteralized.md)
  
  * Distributed Experiments
    * Descriptive Analytics:
      * [Execution Runtimes](DistributedExperiments.md)
      * [Execution Runtimes with Categorizing Figures Long/Short Running Queries](DistributedExperiments_Long_Short_RunningTime_Queries.md)
      * [Best and Worst Configuration Cominataion (Schema, Storage, Partitioning)](QueryPerformanceforConfigs.md)
    * Diagnostic Analytics:
      * [Diagnostic Analyisis of the results (answering the "why?" question)](DescriptiveAnlaytics.md#diagnostic-analysis)  
    * Prescriptive analysis 
      * ([Individual "Bench-Ranking"](IndividualRankingCriteria.md)):
        * [Relational Schema Ranking Scores](SchemaRanking.md)
        * [Partitioning Techs. Ranking Scores](PartitioningRanking.md)
        * [Storage Backends Ranking Scores](StorageRanking.md)
        * We keep all the intermediary **ranking tables** and logs calculations of all the above ranking plots of the dimensions in this [link](https://docs.google.com/spreadsheets/d/1cff9-IVtg4d113TSkdGOBVCmOt6NCOdrorqFhK04g5E/edit?usp=sharing).
      * ([Combined "Bench-Ranking"](CombinedRankingCriteria.md)):
        * Find and download the combined-Ranking criteria results from this [link](https://docs.google.com/spreadsheets/d/1cff9-IVtg4d113TSkdGOBVCmOt6NCOdrorqFhK04g5E/edit?usp=sharing).
       * Bench-Ranking (i.e Ranking criteria) goodness results:
         * In this [link](https://docs.google.com/spreadsheets/d/1cff9-IVtg4d113TSkdGOBVCmOt6NCOdrorqFhK04g5E/edit?usp=sharing), we keep the ranking goodness metrics/measures and results for the ranking criteria. 
    * **Phase#4** results (Schema Advancments Benchmarking):
      * [Relational Schemata Optimizattion VS BaseLine Schemata Results](OptimizedVsBaselinComparsions.md)  
      
Note, we upload all the experiments calculation sheets in this repo:
- [Download The Results Sheets For all the datsets](https://github.com/DataSystemsGroupUT/SPARKSQLRDFBenchmarking/tree/master/results)
    
- We also share all the [scirpts](https://github.com/DataSystemsGroupUT/SPARKSQLRDFBenchmarking/tree/master/Scripts) that we used in our benchmaking experiments to make all computations and analysis as well as plotting all the figures over the logs of the experiments.
- Results sheets of these scripts calculations are also shared [here](https://github.com/DataSystemsGroupUT/SPARKSQLRDFBenchmarking/tree/master/results) in the reposiory.

