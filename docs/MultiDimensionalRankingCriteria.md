## Multi-Dimensional Ranking Criteria


### Limitations of Single-Dimensional Ranking

The single-diemsnional ranking criteria give better explaininations than descriptive and diagnostic analysis. Indeed, they provide clear insights towards the ranked dimension. The follwoing table shows the top-3 configuration combinations according to the single-dimensional ranking criteria. The table highlights the best performing dimension (marked
by the green color across the same dimension, i.e., vertically). For example, ranking by schema we mark VT (b) as the best; ranking by partitioning we mark the SBP (ii). Finally, we can roughly mark ORC (3) followed by Parquet (4) are the best ones when ranking by format.

However, ranking over one dimension and ignoring the others ends up with selecting different configurations. Figure 7 shows the single-dimensional ranking criteria w.r.t a simple geometrical representation that depicts the triangle subsumed by each ranking criterion (Rs, Rp, and Rf ). The triangle sides present the trade-offs ranking dimensions. The red triangles represent the full ranking optimization, i.e, full rank scores, Rx = 1. The blue triangles in the plots represent the actual ranking scores for the selected configurations. Single-dimensional ranking criteria maximize the score for only one dimension while ignoring the other two dimensions. For instance, ranking by schema dimension in Figure 7 (a) shows how schema is perfectly optimized while ignoring the other two sides (dimensions). The same effect of trade-offs is shown in Figure 7 (a), and (b).



### Bench-ranking as a MO optimization problem

In our experiments, we adopt the standard Pareto front optimization techniques to consider all the experimental dimension altogether. The Pareto front framework conducts
the MO optimization over several dimensions in order to find the best possible parameter configurations for the learning outcome of the highest performance. In particular, we utilized the Non-dominated Sorting Genetic Algorithm (NSGA-II) as one of the most popular Pareto front algorithms in order to find the best configuration combinations in our complex experimental
solution space.



