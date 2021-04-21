## Ranking Goodness

A ranking criterion aims at identifying the configurations that have the overall best results. In practice, We can consider a ranking criterion **"good"** if it does not suggest a low-performing configuration. In other words, we are not interested to be the best at any particular query as long as we are never the worst. Herein, we discuss how can we measure such **goodness**, i.e., **how to evaluate the ranking measure?**.

### Def1: Rank Set
A rank set **R** is an ordered set of elements ordered by a ranking score. A rank index **ri** is the index of a ranked element **i** within a ranking set R , i.e., R|r_i|=i. We denote with
**R^k** the left most subset of R of length **k**, and we denote with **R_x** the rank set calculated according to the Rank score **R_x**. 

This problem is well-known in **Information Retrieval** (IR) applications, where several metrics, e.g., *Precision* and *Recall*, are used to validate the ranking. Nevertheless, the main difference between *IR* and bench-ranking is the lack of ground truth. The most reasonable solution is to employ multiple ranking criteria and compare the prescriptions with the actual experimental results. 
However, this approach falls back to the problem relates to **ranking consensus**. Ranking consensus is different from combined ranking. The former is related to choosing between two preference sets, and the latter is about designing a ranking metric that considers multiple dimensions.
