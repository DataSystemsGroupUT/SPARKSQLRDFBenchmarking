## Ranking Goodness

A ranking criterion aims at identifying the configurations that have the overall best results. In practice, We can consider a ranking criterion **"good"** if it does not suggest a low-performing configuration. In other words, we are not interested to be the best at any particular query as long as we are never the worst. Herein, we discuss how can we measure such **goodness**, i.e., **how to evaluate the ranking measure?**.

### Def1: Rank Set
A rank set **R** is an ordered set of elements ordered by a ranking score. A rank index **ri** is the index of a ranked element **i** within a ranking set R , i.e., R|r_i|=i. We denote with
**R^k** the left most subset of R of length **k**, and we denote with **R_x** the rank set calculated according to the Rank score **R_x**. 

This problem is well-known in **Information Retrieval** (IR) applications, where several metrics, e.g., *Precision* and *Recall*, are used to validate the ranking. Nevertheless, the main difference between *IR* and bench-ranking is the lack of ground truth. The most reasonable solution is to employ multiple ranking criteria and compare the prescriptions with the actual experimental results. 
However, this approach falls back to the problem relates to **ranking consensus**. Ranking consensus is different from combined ranking. The former is related to choosing between two preference sets, and the latter is about designing a ranking metric that considers multiple dimensions.


In this regards, **Bench-ranking** proposes to measure the following:

- The ranking **confidence**:by checking how accurate a ranking criterion of its **top-ranked** configurations according to the actual query positioning of those configurations. 
- The ranking **coherence**, that is the **level of agreement** between two ranking sets using different ranking criteria or across different experiments.


To measure the confidence, we propose the following approach described by the following equation:

<div style="text-align:center"> <img src="https://latex.codecogs.com/gif.latex?\begin{equation}\label{eq:cirteriaaccuracyformula}&space;A(\mathcal{R}^{k})=1-\sum&space;_{i=0}^{Q}\sum&space;_{j=0}^{k}&space;\frac{\bar{A}(i,j)}{Q*k},~~~&space;\bar{A}(i,j)=\begin{cases}&space;1&space;&&space;\mathcal{R}^k[j]&space;\in&space;\mathcal{Q}i_h^{i}\\&space;0&space;&&space;otherwise&space;\end{cases}&space;\end{equation}" title="\begin{equation}\label{eq:cirteriaaccuracyformula} A(\mathcal{R}^{k})=1-\sum _{i=0}^{Q}\sum _{j=0}^{k} \frac{\bar{A}(i,j)}{Q*k},~~~ \bar{A}(i,j)=\begin{cases} 1 & \mathcal{R}^k[j] \in \mathcal{Q}i_h^{i}\\ 0 & otherwise \end{cases} \end{equation}" /> </div>

Given the **top-k** subset of the ranking set $$x=5$$  we count how many times its elements occur in the bottom-h subset of the ranking set $\mathcal{Q}_h^{i}$, which corresponds to the ranking set obtained by using the execution time of query $Q_i$ as ranking criterion, for each query. For instance, let's consider the R$_s$ rank and the 100M dataset evaluation. The top-3 ranked configurations are $\mathcal{R}_s^3$=\{b.iii.2,b.iii.1\textbf{,b.iii.4}\} (check \ref{fig:experimentsarch}), that overlaps only with the bottom-3 ranked configurations query Q4. i.e., $\mathcal{Q}4_{3}$=\{b.iii.3,\textbf{b.iii.4},a.iii.2\}.
Thus, $A(\mathcal{R}_s^3)=1-1/(11*3)$.
