### Resutls

- **Bench-Ranking** sheet can be found online as google sheets doc(as it has JS scripts for caluclating ranking goodness, we prefer to keep it online) in this [link](https://docs.google.com/spreadsheets/d/1cff9-IVtg4d113TSkdGOBVCmOt6NCOdrorqFhK04g5E/edit?usp=sharing). In this online sheet, you can find the tables of comparing the ranking criteria, and combined ranking criteria. In addition to a table of the best and worst configuration combinations (schema, partitioning, storage backends).

- We keep local version of the results here also with similar data (**Criteria tables.xlsx**), another one for ranktables and rankscores (**Rank tables and Rank scores.xlsx**).

- In these sheets (online and local) you can find, the execuation running times of queries for the different dimensions, i.e stroage, relational schema, and partitioning techniques Followed by the rank tables, in the final sheet you can find the ranking scores for each dimention. This is can be found for all the datsets (100M, 250M, and 500M).

- Last, For our experiments for the **phase#4** for comparing the optimized relational schemata to the baseline ones, i.e **WPT** Vs **PT** schema and **ExtVP** vs. **VP** schemas  in presensce of **Partitioning** and **Different storage formats**, we uploaded here a combibned sheet of the runtimes logs as well as final results (**FinalResults(Logs and Calculations).xlsx**).

