### Individual Ranking Criteria:

In these regards, ranking criteria, e.g., the one proposed in [akhter2018empirical](https://www.springerprofessional.de/en/an-empirical-evaluation-of-rdf-graph-partitioning-techniques/16257484) for various RDF partitioning techniques, helps provide a high-level view of the performance of a particular dimension across queries. Thus, we have extended the proposed ranking techniques to schemas and storage. The following equation shows a generalized formula for calculating ranking scores.

- **Equation (1)**

<img src="https://latex.codecogs.com/gif.latex?R=\sum_{r=1}^{d}\frac{O_{dim}(r)*(d-r)}{Q(d-1)},&space;0<&space;R&space;\leq&space;1" title="R=\sum_{r=1}^{d}\frac{O_{dim}(r)*(d-r)}{Q(d-1)}, 0< R \leq 1" />


In the above equation, $R$ defines the *Rank Score* of the ranked dimension (i.e., **relational schema**, **partitioning technique**, or **storage backend**). Such that, d represents the total number of variants in the ranked dimension, O_dim(r) denotes the occurrences of the dimension being placed at the rank r (1st,2nd,..), While Q in the formula, represents the total number of query executions, as we have 11 query executions in our SP2Bench benchmark (i.e. Q=11).  

#### Example of Calulating Rank Scores for the different dimensions:

<div style="text-align:center"> <img src="images/RankScoresCalculation.png" width="500" height="250" /> </div>

In the above example, each Rank Score (**R**) value for a dimension is calculated using the Equation (1). Let's take an example, of calcuating R value for the **ST** relational schema. Out of the 11 Queries of the SP2Bench, the **ST** schema acheived the **1st** place (i.e according to Avg. execution runtimes) **1** time, the **2nd** rank **3** times, and as the **3rd** ranked relational schema **7** times. Thus, the calcuation of the equation for ST (Partitioned Horizontally, and Stored as in HDFS Avro backend). 

<img src="https://latex.codecogs.com/gif.latex?R_s(ST)=\sum_{r=1}^{3}\frac{O_{dim}(r)*(3-r)}{11*(3-1)}&space;=\frac{1*(3-1)&plus;3*(3-2)&plus;7*(3-3)}{22}=~0.23" title="R_s(ST)=\sum_{r=1}^{3}\frac{O_{dim}(r)*(3-r)}{11*(3-1)} =\frac{1*(3-1)+3*(3-2)+7*(3-3)}{22}=~0.23" />

Similarly, VT, and PT schemata are ranked using the above equation, but according to their 1st, 2nd, and 3rd occurences, they have differnt Rank-Score values of 0.73, 0.55, respectively.

**Note:** When we apply the generalized ranking formula in Equation (1), we get three rankings for our three mentioned experimental dimensions (Relational Schemata; Partitioning, and Storage Backends), namely, **"R_s"** , **"R_p"**, and **"R_f"** accordingly. 


#### Individual Ranking Criteria (R_s, R_p, and R_f) challenges:

Applying  the  ranking  criteria  independently for each dimension supports explanations of the results [5]. Nevertheless,we observed that ranking prescriptions  are incoherent across dimensions. The most reasonable explanation is that these *mono-dimensional* ranking criteria can not capturea general view, leading to decisive trade-offs.

##### Example that shows the trade-offs among our problem experimental dimensions:

The following table shows  the  best three-ranked configuration combinations. The ”best-ranked” means the configuration combination that shows the highest rank  score according to each ranking criterion (R_s, R_p, and R_f). Looking at the table, we observe that ranking over one of the dimensions provides a better insight  for the decision maker.

<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-c3ow{border-color:inherit;text-align:center;vertical-align:top}
.tg .tg-rvyq{border-color:inherit;font-style:italic;font-weight:bold;text-align:center;vertical-align:top}
</style>
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


Indeed, for each dimension and across scalable datasets, we can mark the best performing dimension. 
- For example, ranking by the dimension of schema, we can mark **VT (b)** as the best. 
- While ranking by partitioning, we can mark the **SBP (ii)** as the best performing. 
- Last but not least, we can mark roughly that **ORC(3)** followed by **Parquet(4)** are the best storage backends.


**Nevertheless**, ranking over one dimension and ignoring the others ends up with selecting different configurations. For instance, ranking over **R_s**, i.e.,relational schema; **R_p**, i.e., partitioning technique, or **R_f**, i.e., the storage backend, end up selecting different combinations of schema, partitioning and storage backends.


In the following figure, we show the separate ranking criteria wrt the geometrical representation of the ranking criteria dimensions. The "Blue triangles" in the plots represent the actual optimization achieved by each ranking criteria. 

Figures show that separate ranking criteria only optimize one dimension, maximizing the corresponding rank, while other dimensions can have non-optimal rank scores. 

<p float="left">
  <img src="images/Rs1.png" width="300" />
  <img src="images/Rf1.png" width="300" /> 
  <img src="images/Rp1.png" width="300" />
</p>
