## Combined Ranking Criteria:

To identify which configuration is the best, we need to optimize along all the dimensions simultaneously. In practice, this means designing a multi-dimensional ranking. To this extent, we propose **three** alternative techniques that aim at combining the ranking dimensions into a single unified ranking criterion.

### Table of contents:
<ul>
  <li><a href="#avg"> The Average (AVG) crtierion</a></li>
  <li><a href="#wavg"> The Weighted Average (WAVG) crtieron</a></li>
  <li><a href="#rta"> The Maximum Triangle Area (Rta)crtierion</a></li>
</ul>


<h3 id="avg"> The Average Criterion</h3>

The **Average (AVG)** criterion leverages an *arithmetic* interpretation of the rankings of our three experimental dimensions. In practice, it aims at maximising their sum and by computing the arithmetic mean of the three rankings (**R_s**, **R_p**, and **R_f**), see the folowing equation.

<div style="text-align:center">  <img src="https://latex.codecogs.com/gif.latex?AVG={\frac&space;{1}{3}}*(R_{f}&plus;R_{p}&plus;R_{s})" title="AVG={\frac {1}{3}}*(R_{f}+R_{p}+R_{s})" /> </div>

<h3 id="wavg"> The Weighted Average </h3>

The **Weighted Average (WAvg)** criterion also leverages an *arithmetic* interpretation of the rankings of our three experimental dimensions. However, it assumes that each dimension contributes differently to the performance. Thus, it requires assigning weights to each individual rank according to its impact in the experiments, e.g., we have 5 different storage backends, 3 partitioning techniques, and 3 relational schemas). (see the following equation)

<div style="text-align:center">  <img src="https://latex.codecogs.com/gif.latex?AVG={\frac&space;{1}{3}}*(R_{f}&plus;R_{p}&plus;R_{s})" title="AVG={\frac {1}{3}}*(R_{f}+R_{p}+R_{s})" /> </div>

<h3 id="rta"> The *Triangle Area* Ranking (Rta) </h3> 

This criterion leverages a geometric interpretation of the rankings of our three experimental dimensions. 
It looks at the triangle subsumed by each ranking criterion (**R_s**, **R_p**, and **R_f**). The **trade-offs** ranking dimensions are presented by the triangle sides. The criterion aims at *maximizing* the area of this triangle (i.e., the <font color="blue">blue</font> triangle) the closer to the ideal (outer red triangle), the better it scores. In other words, the bigger the area of this triangle covers, the better the performance of the three ranking dimensions altogether.

<div style="text-align:center">  <img src="https://user-images.githubusercontent.com/39013653/115556319-3c584780-a2b9-11eb-8f87-2bf962ea0e0c.png" width="400"/> </div>

The following formula computes the actual triangle area. Simply, it sums up the triangle area of the three triangle **A**, **B**, and **C** by two of its sides which are the rank scores of each dimension, i.e **R_s**, **R_p**, or **R_f** (dashed triangle sides), and the angle between both of them (i.e **120** in this case). Then, this triangle area is normalized dividing it by the area of the optimal <font color="red">**red**</font> triangle **D** triangle.  
    
<img src="https://latex.codecogs.com/gif.latex?Triangle&space;Area&space;(Rta)=\frac{1}{2}\sin(120)&space;(R_f*R_p&plus;R_s*R_p&plus;R_f*R_s)" title="Triangle Area (Rta)=\frac{1}{2}\sin(120) (R_f*R_p+R_s*R_p+R_f*R_s)" />

For example, the actual area of the blue triangle of the figure above is calculated as follows:

<img src="https://latex.codecogs.com/gif.latex?Rta&space;=\frac{1}{2}\sin(120)&space;(0.75*0.771&plus;0.73*0.77&plus;0.75*0.73)&space;=&space;0.73" title="Rta =\frac{1}{2}\sin(120) (0.75*0.771+0.73*0.77+0.75*0.73) = 0.73" />



<ul>
  <li style="display:inline;"><a href="https://datasystemsgrouput.github.io/SPARKSQLRDFBenchmarking/Bench-Ranking" style=" margin-right: 50px ;padding: 0px 20px; word-wrap: normal; display: inline-block;   font: bold 11px Arial;  background-color: #EEEEEE;  border-top: 1px solid #CCCCCC;  border-right: 1px solid #333333;  border-bottom: 1px solid #333333;  border-left: 1px solid #CCCCCC;">Previous: Bench-Ranking Criteria</a></li>
  
 <li style="display:inline;"><a href="https://datasystemsgrouput.github.io/SPARKSQLRDFBenchmarking/RankingGoodness.html"  style="padding: 0px 20px; word-wrap: normal; display: inline-block;   font: bold 11px Arial;  background-color: #EEEEEE;  border-top: 1px solid #CCCCCC;  border-right: 1px solid #333333;  border-bottom: 1px solid #333333;  border-left: 1px solid #CCCCCC;">Next:Measurig the Ranking Goodness</a></li>
</ul>
  
