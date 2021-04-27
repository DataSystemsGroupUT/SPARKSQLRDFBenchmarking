## Bench-Ranking: First Step Towards Prescriptive Analyses of Spark-SQL Distributed RDF Data Processing

### Table of contents:
<ul>
  <li><a href="#gartner"> Gartner's Analysis Framework</a></li>
    <ul>
        <li><a href="#desc">Descriptive Analysis</a></li>
        <li><a href="#diag">Diagnosti Analysis</a></li>
        <li><a href="#pred">Predictive Analysis</a></li>
        <li><a href="#pres">Prescriptive Analysis</a></li>
    </ul>
  <li><a href="#motivating"> Motivating Example</a></li>
  <li><a href="#criteria"> Bench-Ranking Criteria</a></li>
      <ul>
        <li><a href="#criteria">Individual Ranking Criteria</a></li>
        <li><a href="#criteria">Combined Ranking Criteria</a></li>
    </ul>
</ul>


Leveraging Big Data (BD) processing frameworks like ApacheSpark-SQL to process large-scale RDF datasets holds a great interest inoptimizing  the  query  performance. 
Modern  BD  services  are  yet;  complicated  data  systems,  where  tuning  the  configurations  notably  affectsthe performance. 
Benchmarking different frameworks and configurationsprovides the community with best practices towards selecting the mostsuitable configurations. 
However, most of these benchmarking efforts areclassified  as  *descriptive*,  *diagnostic*,  or  *predictive*  analytics.  
There is still lack of **prescriptive** and **quantitative** analytics in benchmarking BD applications and systems. **Bench-ranking** takes the first steps in filling this timely research gap. 
In particular, we show the value of prescriptive ranking criteria for evaluating RDF processing systems based on Big Data frameworks.
We validated our proposals with a case-study on Apache Spark-SQL that includes several varying dimensions, i.e. three relational schemata, three partitioning techniques, and five storage backends. 
Selecting the best configuration combination out of this complex solution space is not an easy task. 
The proposed ranking criteria provide an accurate yet simple way that supports the practitioners in this task even in the existence of dimensions' trade-offs.

<h3 id="gartner"> Gartner's Analysis Framework:</h3>

Here, we reflect on the gap of performance analysis in existing works that use Big Data frameworks for RDF processing. 
In particular, we narrow down by discussing the problem of performance analysis alongside a well-known decision-making framework from **Gartner** shown below.


<div style="text-align:center"> <img src="images/Gartner.JPG" width="500" height="250" /> </div>


<h4 id="desc"> Descriptive analysis: </h4>
This level of analysis allows answering factual questions, e.g., *'what happened?'*. This kind of analysis extrapolates *fine-grain* observations that describe a phenomenon through different metrics that could capture its relevant dimensions. 
However, all the work to transform such observations into insights is a subject of the decision maker.

<h4 id="diag">  Diagnostic analysis:</h4>
This level reduces the amount of human intervention by combining the observed data with the *domain knowledge* and, thus, enabling answering explanatory questions like **'why it happened?'**. 
At this level, factual knowledge is contextualized to produce a diagnosis. Typically, diagnostic analysis requires an exploratory phase on existing data and data enrichment. 

<h4 id="pred">  Predictive analysis:</h4>
This level aims to forecast future results, and explain drivers of the observed phenomena using machine learning or data mining techniques. 


<h4 id="pres"> Prescriptive analysis:</h4>
This level reduces the need for human intervention even further by making the insight actionable. In practice, the prescriptive analysis relies on *statistical* and *mathematical* models that aid in answering the question of **'what should be done?'**. 
Regard benchmarking, the prescriptive analysis provides the criteria for selecting the best possible approach given. 


<div style="text-align:center"> <img src="images/AnalyticsLevels.png"  width="400" height="250" /> </div>



<h3 id="motivating">Motivating Example:</h3>

[Descriptive Analyis](DescriptiveAnlaytics.md) present fine-grain observations about the query performance as well as stating which dimentsion is winning (i.e outperforming the others). 
This level was followed by the some diagnosis that describe why this happened (e.g, why VT is in general the best performing relational schema). 
However, these descriptive and diagnostic analyises can't give final answers as we contrdictions indeed occur. Therefore, decision at this level is tricky.

|      | Q1     | Q2      | Q3     | Q4     | Q5      | Q6     | Q7     | Q8      | Q9      | Q10     | Q11            |
|------|--------|---------|--------|--------|---------|--------|--------|---------|---------|---------|----------------|
| 100M | c.i.2  | b.ii.3  | c.ii.3 | a.ii.3 | c.i.5   | c.ii.3 | b.ii.1 | c.iii.4 | b.ii.4  | b.iii.3 |  b.i.3, b.ii.4 |
| 250M | a.i.2  | b.ii.4  | c.ii.4 | a.ii.5 | b.ii.4  | c.ii.3 | b.ii.4 | c.iii.4 | b.iii.3 | b.iii.3 | b.ii.4         |
| 500M | c.ii.2 | b.iii.4 | c.ii.3 | a.ii.3 | b.iii.3 | c.ii.3 | b.ii.4 | c.iii.4 | b.iii.4 | b.iii.3 | b.i.3, b.ii.5  |

For example, the table shown above represents the best configurations for each query and each dataset size. 
A motivation scenario was triggered by analyzing these results, showing no conclusive dimensions to be the most suitable performer. 
The experiment results over different settings also show no decisive configuration setting over the assessed dimensions (i.e., relational schema, partitioning technique, and storage backend), making the practitioner selecting setup a complex task.


<h3 id="criteria"> Bench-Ranking Criteria </h3>

Motivated by the limitations of descriptive and diagnostic analyses, we advocate for indicators such as applying **ranking** techniques for these dimensions.

#### Proposed Ranking Criteria
* [Individual Ranking Criteria](IndividualRankingCriteria.md) 

Genralized Ranking criteria inspiered by ranking partitioning techniques proposed by [akhter2018empirical](https://www.springerprofessional.de/en/an-empirical-evaluation-of-rdf-graph-partitioning-techniques/16257484). We extend this partitoning ranking to cover other two dimensions, mamely, the RDF Relational schemata,and Storage backends. This ranking criteria is optmizing one dimension at a time. 

* [Combined Ranking Criteria](CombinedRankingCriteria.md)

To identify which configuration is the best performing, we need to optimize along all the dimensions simultaneously. In practice, this means designing a multi-dimensional (i.e combined) ranking criteria. To this extent, we propose **three** alternative techniques that aim at combining the ranking dimensions into a single unified ranking criterion. These criteria, are **Averge (AVG)**, **Weighted Average (WAvg)**, and **Maxmized Triangle Area (Rta)** (for all dimensions, each dimesion as a side in the triangle).

#### How to assess the opted ranking criteria ("Bench-Ranking Goodness"):

A ranking criterion aims at identifying the configurations that have the overall best results. In practice, We can consider a ranking criterion **"good"** if it does not suggest a low-performing configuration. In other words, we are not interested to be the best at any particular query as long as we are never the worst. Herein, we discuss how can we measure such **goodness**, i.e., **how to evaluate the ranking measure?**. [Ranking Goodness Details and Measures](RankingGoodness.md).
