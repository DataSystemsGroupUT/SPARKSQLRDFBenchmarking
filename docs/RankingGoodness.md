## Ranking Goodness

A ranking criterion aims at identifying the configurations that have the overall best results. In practice, We can consider a ranking criterion **"good"** if it does not suggest a low-performing configuration. In other words, we are not interested to be the best at any particular query as long as we are never the worst. Herein, we discuss how can we measure such **goodness**, i.e., **how to evaluate the ranking measure?**.

### Table of contents:
<ul>
  <li><a href="#goodcri"> The Proposed Goodness Measures </a></li>
  <ul>
    <li><a href="#confidence">  The Conformance Measure  </a></li>
    <ul>
      <li><a href="#exampleconfidence">  Examples of the Confidence measure </a></li>
    </ul>
    <li><a href="#coherence"> The Proposed Goodness Measures </a></li>
        <ul>
      <li><a href="#examplecoherence">  Examples of the Coherence measure  </a></li>
    </ul>
  </ul>
  

</ul>


This problem is well-known in **Information Retrieval** (IR) applications, where several metrics, e.g., *Precision* and *Recall*, are used to validate the ranking. Nevertheless, the main difference between *IR* and bench-ranking is the lack of ground truth. The most reasonable solution is to employ multiple ranking criteria and compare the prescriptions with the actual experimental results. 
However, this approach falls back to the problem relates to **ranking consensus**. Ranking consensus is different from combined ranking. The former is related to choosing between two preference sets, and the latter is about designing a ranking metric that considers multiple dimensions.


##### Def1: Rank Set
A rank set **R** is an ordered set of elements ordered by a ranking score. A rank index **ri** is the index of a ranked element **i** within a ranking set R , i.e., R|r_i|=i. We denote with **R^k** the left most subset of R of length **k**, and we denote with **R_x** the rank set calculated according to the Rank score **R_x**. 


<h3 id="goodcri"> Goodness Criteria: </h3>
In this regards, **Bench-ranking** proposes to measure the following:

- The ranking **confidence**:by checking how accurate a ranking criterion of its **top-ranked** configurations according to the actual query positioning of those configurations. 
- The ranking **coherence**, that is the **level of agreement** between two ranking sets using different ranking criteria or across different experiments. The invariant in our case is the Scale (across different dataset sizes from 100-to-250, 250-500,100-500M datsets).


<h4 id="confidence">  1- The Conformance Measure </h4>

To measure the **confidence**, we propose the following approach described by the following equation:

<div style="text-align:center"> <img src="https://latex.codecogs.com/gif.latex?\begin{equation}\label{eq:cirteriaaccuracyformula}&space;A(\mathcal{R}^{k})=1-\sum&space;_{i=0}^{Q}\sum&space;_{j=0}^{k}&space;\frac{\bar{A}(i,j)}{Q*k},~~~&space;\bar{A}(i,j)=\begin{cases}&space;1&space;&&space;\mathcal{R}^k[j]&space;\in&space;\mathcal{Q}i_h^{i}\\&space;0&space;&&space;otherwise&space;\end{cases}&space;\end{equation}" title="\begin{equation}\label{eq:cirteriaaccuracyformula} A(\mathcal{R}^{k})=1-\sum _{i=0}^{Q}\sum _{j=0}^{k} \frac{\bar{A}(i,j)}{Q*k},~~~ \bar{A}(i,j)=\begin{cases} 1 & \mathcal{R}^k[j] \in \mathcal{Q}i_h^{i}\\ 0 & otherwise \end{cases} \end{equation}" /> </div>

Given the **top-k** subset of the ranking set R we count how many times its elements occur in the bottom-h subset of the ranking set Q_h(i), which corresponds to the ranking set obtained by using the execution time of query **Q_i** as ranking criterion, for each query. In other words, we look at the rank of the top-ranked configurations (by a ranking criteria R), and make sure by the above formula that they are not in the bottm-h query positions/ranked configurations. This is computed for all queries in the benchmark. the intuition is the ranking top ranked configuration shouldn't appear as worst performing for the queries.


**Table of best 3 configuration ranked by the criteria Rf,Rp, and Rs (single-dimensional criteria), across our experimental datasets**
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

<h5 id="exampleconfidence"> Examples of the confidence Calculations</h5>

For instance, let's consider the **R_s** rank and the **100M** dataset evaluation. The top-3 ranked configurations (see the table above) are R_s (Top-3)={b.iii.2,b.iii.1, **b.iii.4**}} that overlaps only with the **bottom-3** ranked configurations query Q4, i.e., Q4_(bottom-3)={b.iii.3,b.iii.4,a.iii.2}. Thus, A(R_s [3])=1- (1/(11*3))=0.969.



To get a visualized intuition behind the conformance, The following figure shows the level of conformance of the top-ranked three configurations for the single-dimensional as well as pareto ranking criteria. 

<img src="images/conformancechart.png" alt="conformance" >


**Example on one of the datsets (100M):**

Table below shows the "confidence" ratios calcuated for all the ranking criteria (i.e, [individual](IndividualRankingCriteria.md) **R_f, R_s,R_p**, and [combined](CombinedRankingCriteria.md) **AVG, WAvg, Rta**). The table show the top-3 ranked configurations for each criteria, alongside all the the benchmark queris ranking of these configurations. The column **"rank > 22"** checks whehther these configurations opted by each criteria are not worst than the **22** bottom-ranked configurations according to the queries. Notably, this **22** is arbitraily used, but we can use any number *restricting** or *relaxing* the confidence calculation in the formula. In these [sheets](https://docs.google.com/spreadsheets/d/1cff9-IVtg4d113TSkdGOBVCmOt6NCOdrorqFhK04g5E/edit?usp=sharing), you can find other examples of calculating the confidence with different **h-bottom** values other than *22* example.



<!-- <table class="tg">
<thead>
  <tr>
    <th class="tg-7btt" colspan="12">Confidence of each Ranking Criteria</th>
    <th class="tg-fymr" rowspan="3">rank &gt; 22</th>
    <th class="tg-fymr" rowspan="3">A(R)</th>
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
</table> -->



<table class="tg">
<thead>
  <tr>
    <th class="tg-7btt" colspan="12">Confidence of each Ranking Criteria</th>
    <th class="tg-fymr" rowspan="3">rank &gt; 17</th>
    <th class="tg-fymr" rowspan="3">A(R)</th>
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
    <td class="tg-8bgf" colspan="12"><span style="font-weight:bold;background-color:#6D9EEB">Pareto_agg</span></td>
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
    <td class="tg-8bgf" colspan="12"><span style="font-weight:bold;background-color:#6D9EEB">Pareto_Q</span></td>
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
  
</tbody>
</table>




**Note that:** the above results show the conformance for the configurations considering the Hive backend as a 5th stroage backend. However, for consistency we ommitted Hive from the calculations and kept only the HDFS file formats (CSV, Avro, ORC, and Parquet). Thus, we have new calculations and the configurations become 36 instead of 45 (i.e., by excluding configurations that include Hive as a storage backend).


The following table shows the conformance of each ranking criterion **top-3** configurations not being worse than the worst the 17 ranked configurations (i.e., better than the 17 ones, half of the distri-bution) according to the queries’ ranked sets.


|            | 100M | 250M  | 500M |
|------------|------|-------|------|
| R_f        | 58\% |  82\% | 70\% |
| R_p        | 61\% |  70\% | 58\% |
| R_s        | 70\% |  79\% | 79\% |
| Pareto_Agg | 79\% | 100\% | 82\% |
| Pareto_{Q} | 85\% | 100\% | 97\% |


All the selected ranking criteria perform very well for all the datasets. However, the single-dimensional criteria Rf , Rp, and Rs have lower conformance than the one based on Pareto. For instance, in the 100M, 250M, and 500M datasets, ParetoAgg. has a conformance of 79%, 100%, and 82%, respectively. The same pattern repeats with the ParetoQ version (with 85%,
100%, and 97%, respectively). In contrast, single-dimensional ranking criteria have relatively lower conformance of 58%, 82%, and 70% for Rf , 61%, 70%, and 58% for Rp, and 70%,
79%, and 79% for Rs, accordingly. 

The Conformance Figure shown above also depicts the level of conformance in green color, for the top-3 ranked configuration combinations. We can see that the level of conformance in the pareto raking criteria (multi-dimensional) is higher than the Rf, Rp, Rs single-dimensinal ranking criteria.  

The main reason behind these results is that single-dimensional criteria do not consider trade-offs across experimental dimensions, ultimately selecting the configuration that may under-perform in some queries. Meanwhile, Pareto-based ranking considers those trade-offs while optimizing all the dimensions simultaneously.


<h4 id="coherence">  2- The Coherence Measure: </h4>
To measure the coherence of each ranking criterion, we opt for **Kendall index**, which counts the number of pairwise disagreements between two rank sets: the larger the distance, the more dissimilar the rank sets are. Notably, we assume that rank sets have the same number of elements. Kendall’s distance between two rank sets R_1 and R_2, where P represent the set of unique pairs of distinct elements in the two sets can be calculated using the following equation:


<img src="https://latex.codecogs.com/gif.latex?\begin{equation}\label{eq:kendall}&space;K\left(\mathcal{R}1,&space;\mathcal{R}2\right)=\sum_{\{i,&space;j\}&space;\in&space;P}\frac{\bar{K}_{i,&space;j}\left(\mathcal{R}1,&space;\mathcal{R}2\right)}{\mid&space;P&space;\mid}&space;\end{equation}&space;\\&space;\\&space;\bar{K}_{i,&space;j}\left(\mathcal{R}1,&space;\mathcal{R}2\right&space;)=&space;\begin{cases}&space;0&space;&&space;\mathcal{R}1[r^1_i]=&space;\mathcal{R}2[r^2_i]=&space;i&space;~\wedge&space;\mathcal{R}1[r^1_j]=&space;\mathcal{R}2[r^2_j]=&space;j&space;~\wedge&space;r^1_i-r^1_j&space;=&space;r^2_i&space;-&space;r^2_j\\&space;1&space;&&space;\text{otherwise}&space;\end{cases}\" title="\begin{equation}\label{eq:kendall} K\left(\mathcal{R}1, \mathcal{R}2\right)=\sum_{\{i, j\} \in P}\frac{\bar{K}_{i, j}\left(\mathcal{R}1, \mathcal{R}2\right)}{\mid P \mid} \end{equation} \\ \\ \bar{K}_{i, j}\left(\mathcal{R}1, \mathcal{R}2\right )= \begin{cases} 0 & \mathcal{R}1[r^1_i]= \mathcal{R}2[r^2_i]= i ~\wedge \mathcal{R}1[r^1_j]= \mathcal{R}2[r^2_j]= j ~\wedge r^1_i-r^1_j = r^2_i - r^2_j\\ 1 & \text{otherwise} \end{cases}\" />

<h5 id="examplecoherence"> Example of Coherence </h5>

For instance, the Kendall's index (**K**) between **R_s (Top-3)** for **100M** and **250M** is **0.33** (See table above of best_ranked_configuration for the individual criteria in the page). Indeed, there was only one disagreement out of three configurations observations (i.e, the cofiguration **b.ii.1** in the 250M dataset ranked better than **b.iii.2**).

Table below shows the **K** index as per Equation (2) in this page. All the criteria show a good coherence across different scales of the datasets (the lower the better). Indeed, scaling the datasets up, we realize small distances (i.e. indicating low changes in the ranking ordinals) for both individual and combined ranking criteria. Intuitively, if across scalability, the opted ranking criterion has high Kendall's index (i.e., high disagreement of the same ranking), it indicates the **inappropriateness** for ranking to describe the performance.



To give the intuituion behind the coherence metric by showing the top-10 ranked configuration for the same ranking criterion(R_p) (i.e., <img src="https://latex.codecogs.com/gif.latex?R^{10} " />)
and across three different data scales. We can see examples of pairwise disagreements that occur by scaling from 100M to 250M, and also from 100M to the 500M dataset. For instance, in (100M -to-250M ), b.ii.3 was at the 10th rank in 100M, while being swapped to be at the 1st position in the 250M, and the 1st ranked configuration (a.ii.3) in the 100M swapped to be at the 8th rank in the 250M. Similar kind of disagreements are shown in the (100M-to-500M) scale-up transition.


<img src="images/coherence.jpeg" alt="coherence" >

Please note the swaps of colors, it starts as blue, red, blue,..., however with moving to larger data scales, color disagreements already occur there with the mentioned swaps.


<!-- <table class="tg">
<thead>
  <tr>
    <th class="tg-7btt">Dataset_i VS. Dataset_j</th>
    <th class="tg-rvyq">R_f</th>
    <th class="tg-rvyq">R_s</th>
    <th class="tg-rvyq">R_p</th>
    <th class="tg-rvyq">AVG</th>
    <th class="tg-rvyq">WAvg</th>
    <th class="tg-rvyq">R_ta</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-rvyq">100M vs 250M</td>
    <td class="tg-c3ow">0.17</td>
    <td class="tg-c3ow">0.07</td>
    <td class="tg-c3ow">0.17</td>
    <td class="tg-c3ow">0.18</td>
    <td class="tg-c3ow">0.18</td>
    <td class="tg-c3ow">0.17</td>
  </tr>
  <tr>
    <td class="tg-rvyq">100M vs 500M</td>
    <td class="tg-c3ow">0.17</td>
    <td class="tg-c3ow">0.07</td>
    <td class="tg-c3ow">0.28</td>
    <td class="tg-c3ow">0.19</td>
    <td class="tg-c3ow">0.20</td>
    <td class="tg-c3ow">0.19</td>
  </tr>
  <tr>
    <td class="tg-rvyq">250M vs 500M</td>
    <td class="tg-c3ow">0.17</td>
    <td class="tg-c3ow">0.09</td>
    <td class="tg-c3ow">0.21</td>
    <td class="tg-c3ow">0.17</td>
    <td class="tg-c3ow">0.17</td>
    <td class="tg-c3ow">0.15</td>
  </tr>
</tbody>
</table>
 -->



 <table class="tg">
<thead>
  <tr>
    <th class="tg-7btt">Dataset_i VS. Dataset_j</th>
    <th class="tg-rvyq">R_f</th>
    <th class="tg-rvyq">R_s</th>
    <th class="tg-rvyq">R_p</th>
    <th class="tg-rvyq">Pareto_Agg</th>
    <th class="tg-rvyq">Pareto_Q</th>
<!--     <th class="tg-rvyq">AVG</th>
    <th class="tg-rvyq">WAvg</th>
    <th class="tg-rvyq">R_ta</th> -->
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-rvyq">100M vs 250M</td>
    <td class="tg-c3ow">0.13</td>
    <td class="tg-c3ow">0.18</td>
    <td class="tg-c3ow">0.06</td>
    <td class="tg-c3ow">0.19</td>
    <td class="tg-c3ow">0.24</td>
<!--     <td class="tg-c3ow">0.17</td> -->
  </tr>
  <tr>
    <td class="tg-rvyq">100M vs 500M</td>
    <td class="tg-c3ow">0.16</td>
    <td class="tg-c3ow">0.29</td>
    <td class="tg-c3ow">0.06</td>
    <td class="tg-c3ow">0.19</td>
    <td class="tg-c3ow">0.24</td>
<!--     <td class="tg-c3ow">0.19</td> -->
  </tr>
  <tr>
    <td class="tg-rvyq">250M vs 500M</td>
    <td class="tg-c3ow">0.13</td>
    <td class="tg-c3ow">0.19</td>
    <td class="tg-c3ow">0.07</td>
    <td class="tg-c3ow">0.13</td>
    <td class="tg-c3ow">0.18</td>
<!--     <td class="tg-c3ow">0.15</td> -->
  </tr>
</tbody>
</table>


The above table shows the results where the reading key is the lower, the better( i.e., high Kendall’s index means high disagreement across rank set). All the ranking criteria show high coherence across different scales of the datasets. Indeed, scaling the datasets does not excessively impact the rank sets’ order in all the ranking
criteria.



**Note** All the scripts for calculating the Kendall's index can be found in the [scripts](https://github.com/DataSystemsGroupUT/SPARKSQLRDFBenchmarking/tree/master/scripts) in this repo, and attached with the calculations [sheet](https://docs.google.com/spreadsheets/d/1cff9-IVtg4d113TSkdGOBVCmOt6NCOdrorqFhK04g5E/edit?usp=sharing).



<ul>
  <li style="display:inline;"><a href="https://datasystemsgrouput.github.io/SPARKSQLRDFBenchmarking/IndividualRankingCriteria.html" style=" margin-right: 50px ;padding: 0px 20px; word-wrap: normal; display: inline-block;   font: bold 11px Arial;  background-color: #EEEEEE;  border-top: 1px solid #CCCCCC;  border-right: 1px solid #333333;  border-bottom: 1px solid #333333;  border-left: 1px solid #CCCCCC;">Single-Dimensional Ranking Criteria</a></li>
  
 <li style="display:inline;"><a href="https://datasystemsgrouput.github.io/SPARKSQLRDFBenchmarking/MultiDimensionalRankingCriteria.html"  style="padding: 0px 20px; word-wrap: normal; display: inline-block;   font: bold 11px Arial;  background-color: #EEEEEE;  border-top: 1px solid #CCCCCC;  border-right: 1px solid #333333;  border-bottom: 1px solid #333333;  border-left: 1px solid #CCCCCC;">Multi-Dim. Ranking Criteria</a></li>
</ul>
  


