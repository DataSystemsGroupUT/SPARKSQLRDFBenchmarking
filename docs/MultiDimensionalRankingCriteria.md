## Multi-Dimensional Ranking Criteria

<!-- 
### Limitations of Single-Dimensional Ranking

The single-diemsnional ranking criteria give better explaininations than descriptive and diagnostic analysis. Indeed, they provide clear insights towards the ranked dimension. The follwoing table shows the top-3 configuration combinations according to the single-dimensional ranking criteria. The table highlights the best performing dimension (marked
by the green color across the same dimension, i.e., vertically). For example, ranking by schema we mark VT (b) as the best; ranking by partitioning we mark the SBP (ii). Finally, we can roughly mark ORC (3) followed by Parquet (4) are the best ones when ranking by format.

However, ranking over one dimension and ignoring the others ends up with selecting different configurations. Figure 7 shows the single-dimensional ranking criteria w.r.t a simple geometrical representation that depicts the triangle subsumed by each ranking criterion (Rs, Rp, and Rf ). The triangle sides present the trade-offs ranking dimensions. The red triangles represent the full ranking optimization, i.e, full rank scores, Rx = 1. The blue triangles in the plots represent the actual ranking scores for the selected configurations. Single-dimensional ranking criteria maximize the score for only one dimension while ignoring the other two dimensions. For instance, ranking by schema dimension in Figure 7 (a) shows how schema is perfectly optimized while ignoring the other two sides (dimensions). The same effect of trade-offs is shown in Figure 7 (a), and (b).
 -->

The single-diemsnional ranking criteria give better explaininations than descriptive and diagnostic analysis. Indeed, they provide clear insights towards the ranked dimension. However, ranking over one dimension and ignoring the others ends up with selecting different configurations. This intuition leads to extend the Bench-ranking into a multiobjective optimization problem in order to optimize all the dimensions at the same time.

### Bench-ranking as a MO optimization problem

In our experiments, we adopt the standard Pareto front optimization techniques to consider all the experimental dimension altogether. The Pareto front framework conducts
the MO optimization over several dimensions in order to find the best possible parameter configurations for the learning outcome of the highest performance. In particular, we utilized the Non-dominated Sorting Genetic Algorithm (NSGA-II) as one of the most popular Pareto front algorithms in order to find the best configuration combinations in our complex experimental
solution space. Furthermore, MO optimization techniques can handle any arbitrary number of dimensions and objective functions.


### Multi-Dimensional Ranking Criteria in Paractice

In our experiments, we calculate the Pareto fronts for our Bench-Ranking problem in two ways.

The first way, which we call (ParetoQ), applied NSGA-II algorithm considering the rank sets obtained by sorting w.r.t each query result individually (see Table IV). The algorithm aims at minimizing the query runtimes globally. 

The second way, called (ParetoAgg.), operates on the single-dimensional ranking criteria, i.e., Rs, Rp, and Rf . In this case, the algorithm aims at maximizing the performance of the three ranks altogether. Notably, for both implementations of the algorithm, the search space is given in the form of configurations’ query runtimes (configurations query positions, see Table IV), or in configurations’ rank scores (Rs, Rf , Rp) in the aggregated form of Pareto, instead of generating a search space.

### Pareto Results

The following table shows the Pareto fronts for both approaches, i.e., non-dominated solutions, which correspond to the overall optimal configurations for the 100M, 250M, and 500M triples datasets.



|        500M    |    |        |         |         |         |         |        |         |        |        |        |       |         |       |        |         |
|------------|:------:|:------:|:-------:|:-------:|:-------:|:-------:|:------:|:-------:|:------:|:------:|:------:|:-----:|:-------:|:-----:|:------:|:-------:|
| Pareto_Agg | c.ii.4 | b.ii.4 | b.iii.1 | b.iii.3 | b.iii.4 | b.ii.3  | a.ii.3 | a.iii.3 | b.i.4  | -      | -      | -     | -       | -     | -      | -       |
| Pareto_Q   | b.ii.3 | b.ii.4 | b.iii.3 | b.iii.4 | b.i.3   | b.iii.1 | b.i.4  | b.ii.1  | c.ii.4 | c.ii.3 | a.ii.3 | c.i.3 | a.iii.3 | c.i.2 | c.ii.2 | c.iii.4 |





