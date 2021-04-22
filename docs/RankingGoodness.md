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


To measure the **confidence**, we propose the following approach described by the following equation:

<div style="text-align:center"> <img src="https://latex.codecogs.com/gif.latex?\begin{equation}\label{eq:cirteriaaccuracyformula}&space;A(\mathcal{R}^{k})=1-\sum&space;_{i=0}^{Q}\sum&space;_{j=0}^{k}&space;\frac{\bar{A}(i,j)}{Q*k},~~~&space;\bar{A}(i,j)=\begin{cases}&space;1&space;&&space;\mathcal{R}^k[j]&space;\in&space;\mathcal{Q}i_h^{i}\\&space;0&space;&&space;otherwise&space;\end{cases}&space;\end{equation}" title="\begin{equation}\label{eq:cirteriaaccuracyformula} A(\mathcal{R}^{k})=1-\sum _{i=0}^{Q}\sum _{j=0}^{k} \frac{\bar{A}(i,j)}{Q*k},~~~ \bar{A}(i,j)=\begin{cases} 1 & \mathcal{R}^k[j] \in \mathcal{Q}i_h^{i}\\ 0 & otherwise \end{cases} \end{equation}" /> </div>

Given the **top-k** subset of the ranking set R we count how many times its elements occur in the bottom-h subset of the ranking set Q_h(i), which corresponds to the ranking set obtained by using the execution time of query **Q_i** as ranking criterion, for each query. In other words, we look at the rank of the top-ranked configurations (by a ranking criteria R), and make sure by the above formula that they are not in the bottm-h query positions/ranked configurations. This is computed for all queries in the benchmark. the intuition is the ranking top ranked configuration shouldn't appear as worst performing for the queries.



<table class="tg">
<thead>
  <tr>
    <th class="tg-rvyq">Top-3 Configurations</th>
    <th class="tg-rvyq" colspan="3">100M</th>
    <th class="tg-rvyq" colspan="3">250M</th>
    <th class="tg-rvyq" colspan="3">500M</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-rvyq">Rs</td>
    <td class="tg-c3ow">b.iii.2</td>
    <td class="tg-c3ow">b.iii.1</td>
    <td class="tg-c3ow">b.iii.4</td>
    <td class="tg-c3ow">b.iii.1</td>
    <td class="tg-c3ow">b.iii.2</td>
    <td class="tg-c3ow">b.iii.3</td>
    <td class="tg-c3ow">b.iii.1</td>
    <td class="tg-c3ow">b.iii.2</td>
    <td class="tg-c3ow">b.iii.4</td>
  </tr>
  <tr>
    <td class="tg-rvyq">Rp</td>
    <td class="tg-c3ow">a.ii.3</td>
    <td class="tg-c3ow">a.ii.4</td>
    <td class="tg-c3ow">a.ii.5</td>
    <td class="tg-c3ow">a.ii.5</td>
    <td class="tg-c3ow">b.ii.3</td>
    <td class="tg-c3ow">c.ii.3</td>
    <td class="tg-c3ow">c.ii.3</td>
    <td class="tg-c3ow">c.ii.4</td>
    <td class="tg-c3ow">b.ii.5</td>
  </tr>
  <tr>
    <td class="tg-rvyq">Rf</td>
    <td class="tg-c3ow">a.iii.3</td>
    <td class="tg-c3ow">a.ii.3</td>
    <td class="tg-c3ow">c.ii.3</td>
    <td class="tg-c3ow">a.iii.3</td>
    <td class="tg-c3ow">a.ii.3</td>
    <td class="tg-c3ow">b.ii.4</td>
    <td class="tg-c3ow">a.ii.3</td>
    <td class="tg-c3ow">a.iii.3</td>
    <td class="tg-c3ow">b.i.4</td>
  </tr>
</tbody>
</table>

Example: For instance, let's consider the **R_s** rank and the **100M** dataset evaluation. The top-3 ranked configurations (see the table above) are R_s (Top-3)={b.iii.2,b.iii.1, **b.iii.4**}} that overlaps only with the **bottom-3** ranked configurations query Q4, i.e., Q4_(bottom-3)={b.iii.3,b.iii.4,a.iii.2}. Thus, A(R_s [3])=1- (1/(11*3))=0.969.


Table below shows the "confidence" ratios calcuated for all the ranking criteria (i.e, [individual]() R_f, R_s,R_p, and [combined]() AVG, WAvg, Rta). The table show the top-3 ranked configurations for each criteria, alongside all the the benchmark queris ranking of these configurations. The column **"Top ranked > 22"** checks whehther these configurations opted by each criteria are not worst than the **22** bottom-ranked configurations according to the queries. Notably, this **22** is arbitraily used, but we can use any number *restricting** or *relaxing* the confidence calculation in the formula. In these [sheets](https://docs.google.com/spreadsheets/d/1cff9-IVtg4d113TSkdGOBVCmOt6NCOdrorqFhK04g5E/edit?usp=sharing), you can find other examples of calculating the confidence with different **h-bottom** values other than *22* example.



<table class="tg">
<thead>
  <tr>
    <th class="tg-7btt" colspan="12">Confidence of each Ranking Criteria</th>
    <th class="tg-fymr" rowspan="3">Top_Ranked &gt; 22</th>
    <th class="tg-fymr" rowspan="3">Confidence</th>
  </tr>
  <tr>
    <td class="tg-fymr">100M</td>
    <td class="tg-fymr"><span style="font-weight:bold;background-color:#FFF">Q1</span></td>
    <td class="tg-fymr"><span style="font-weight:bold;color:#000;background-color:#FFF">Q2</span></td>
    <td class="tg-fymr"><span style="font-weight:bold;color:#000;background-color:#FFF">Q3</span></td>
    <td class="tg-fymr"><span style="font-weight:bold;background-color:#FFF">Q4</span></td>
    <td class="tg-fymr"><span style="font-weight:bold;background-color:#FFF">Q5</span></td>
    <td class="tg-fymr"><span style="font-weight:bold;background-color:#FFF">Q6</span></td>
    <td class="tg-fymr"><span style="font-weight:bold;background-color:#FFF">Q7</span></td>
    <td class="tg-fymr"><span style="font-weight:bold;background-color:#FFF">Q8</span></td>
    <td class="tg-fymr"><span style="font-weight:bold;background-color:#FFF">Q9</span></td>
    <td class="tg-fymr"><span style="font-weight:bold;background-color:#FFF">Q10</span></td>
    <td class="tg-fymr"><span style="font-weight:bold;background-color:#FFF">Q11</span></td>
  </tr>
  <tr>
    <td class="tg-rvyq" colspan="12"><span style="background-color:#6D9EEB">Rf</span></td>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-f8tv">a.iii.3</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">29</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">20</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">32</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">26</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">27</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">30</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">41</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">20</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">22</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">11</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">6</span></td>
    <td class="tg-c3ow" rowspan="3"><span style="font-weight:400;font-style:normal">0.64</span></td>
  </tr>
  <tr>
    <td class="tg-f8tv">a.ii.3</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">26</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">25</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">5</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">16</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">2</span></td>
  </tr>
  <tr>
    <td class="tg-f8tv"><span style="background-color:#FFF">c.ii.3</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">15</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">15</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">5</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">31</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">3</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">31</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">27</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">26</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">4</span></td>
  </tr>
  <tr>
    <td class="tg-8bgf" colspan="12"><span style="font-weight:bold;background-color:#6D9EEB">Rp</span></td>
    <td class="tg-c3ow" colspan="2"></td>
  </tr>
  <tr>
    <td class="tg-f8tv">a.ii.3</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">26</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">25</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">5</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">16</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">2</span></td>
    <td class="tg-c3ow" rowspan="3">0.61</td>
  </tr>
  <tr>
    <td class="tg-f8tv">a.ii.5</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">27</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">24</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">26</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">13</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">27</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">16</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">24</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">7</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">19</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">5</span></td>
  </tr>
  <tr>
    <td class="tg-f8tv">a.ii.4</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">23</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">23</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">24</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">4</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">24</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">26</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">19</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">23</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">8</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">13</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">21</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">6</span></td>
  </tr>
  <tr>
    <td class="tg-8bgf" colspan="12"><span style="font-weight:bold;background-color:#6D9EEB">Rs</span></td>
    <td class="tg-c3ow" colspan="2"></td>
  </tr>
  <tr>
    <td class="tg-f8tv">b.iii.2</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">5</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">22</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">29</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">32</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">19</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">11</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">19</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">25</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">15</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">3</span></td>
    <td class="tg-c3ow" rowspan="3">0.73</td>
  </tr>
  <tr>
    <td class="tg-f8tv">b.iii.1</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">41</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">12</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">30</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">12</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">2</span></td>
  </tr>
  <tr>
    <td class="tg-f8tv">b.iii.4</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">12</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">6</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">23</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">44</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">23</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">22</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">36</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">4</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">7</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">10</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">4</span></td>
  </tr>
  <tr>
    <td class="tg-8bgf" colspan="12"><span style="font-weight:bold;background-color:#6D9EEB">AVG</span></td>
    <td class="tg-c3ow" colspan="2"></td>
  </tr>
  <tr>
    <td class="tg-f8tv"><span style="background-color:#FFF">b.ii.4</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">10</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">12</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">27</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">7</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">6</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">8</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">1</span></td>
    <td class="tg-c3ow" rowspan="3">0.88</td>
  </tr>
  <tr>
    <td class="tg-f8tv">a.ii.3</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">26</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">25</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">5</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">16</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">2</span></td>
  </tr>
  <tr>
    <td class="tg-f8tv">b.ii.5</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">16</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">10</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">28</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">7</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">16</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">6</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">4</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">1</span></td>
  </tr>
  <tr>
    <td class="tg-8bgf" colspan="12"><span style="font-weight:bold;background-color:#6D9EEB">WAvg</span></td>
    <td class="tg-c3ow" colspan="2"></td>
  </tr>
  <tr>
    <td class="tg-f8tv">a.ii.3</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">26</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">25</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">5</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">16</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">2</span></td>
    <td class="tg-c3ow" rowspan="3">0.79</td>
  </tr>
  <tr>
    <td class="tg-f8tv">b.ii.4</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">10</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">12</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">27</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">7</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">6</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">8</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">1</span></td>
  </tr>
  <tr>
    <td class="tg-f8tv"><span style="background-color:#FFF">c.ii.3</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">15</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">15</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">5</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">31</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">3</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">31</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">27</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">26</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">4</span></td>
  </tr>
  <tr>
    <td class="tg-8bgf" colspan="12"><span style="font-weight:bold;background-color:#6D9EEB">Rta</span></td>
    <td class="tg-0pky" colspan="2"></td>
  </tr>
  <tr>
    <td class="tg-f8tv">b.ii.4</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">10</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">12</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">27</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">7</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">6</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">8</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">1</span></td>
    <td class="tg-c3ow" rowspan="3"><span style="font-weight:400;font-style:normal">0.88</span></td>
  </tr>
  <tr>
    <td class="tg-f8tv">b.ii.5</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">16</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">10</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">28</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">7</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">16</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">2</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">6</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">4</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">1</span></td>
  </tr>
  <tr>
    <td class="tg-f8tv">a.ii.3</td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">26</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">14</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">18</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">1</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">25</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">17</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">5</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">9</span></td>
    <td class="tg-c6of"><span style="font-weight:normal;color:#000">16</span></td>
    <td class="tg-c3ow"><span style="font-weight:normal;color:#000;background-color:#FFF">2</span></td>
  </tr>
</tbody>
</table>
