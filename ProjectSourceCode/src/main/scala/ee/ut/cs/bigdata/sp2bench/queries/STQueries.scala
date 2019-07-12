package ee.ut.cs.bigdata.sp2bench.queries

class STQueries {




  //Q1  100%

val q1=
  """
    |SELECT
    |  T3.object AS Year
    |
    |  FROM SingleStmtTable T1, SingleStmtTable T2, SingleStmtTable T3
    |    WHERE T1.subject=T2.subject
    |  AND   T2.subject=T3.subject
    |  AND T1.object='http://localhost/vocabulary/bench/Journal'
    |  AND T2.predicate='http://purl.org/dc/elements/1.1/title'
    |  AND T2.object='Journal 1 (1940)'
    |  AND T3.predicate='http://purl.org/dc/terms/issued'
  """.stripMargin


  // Q2    2,149   xrH3b6b  100%


val q2=
"""
  |SELECT
  |
  |  T1.subject AS inproc, T2.object AS author, T3.object AS booktitle, T7.object AS title, T4.object AS proc,
  |  T5.object AS ee, T6.object AS page, T8.object AS homepage, T9.object AS issued, AB.object AS abstract
  |
  |  FROM SingleStmtTable T1
  |  JOIN SingleStmtTable T2     ON T1.subject=T2.subject
  |  JOIN SingleStmtTable T3     ON T1.subject=T3.subject
  |  JOIN SingleStmtTable T4     ON T1.subject=T4.subject
  |  JOIN SingleStmtTable T5     ON T1.subject=T5.subject
  |  JOIN SingleStmtTable T6     ON T1.subject=T6.subject
  |  JOIN SingleStmtTable T7     ON T1.subject=T7.subject
  |  JOIN SingleStmtTable T8     ON T1.subject=T8.subject
  |  JOIN SingleStmtTable T9     ON T1.subject=T9.subject
  |
  |  LEFT JOIN (
  |    SELECT *
  |      FROM
  |      SingleStmtTable T10
  |      WHERE
  |      T10.predicate='http://localhost/vocabulary/bench/abstract'
  |  ) AB ON T1.subject=AB.subject
  |
  |  WHERE
  |  T1.object='http://localhost/vocabulary/bench/Inproceedings'
  |  AND T2.predicate='http://purl.org/dc/elements/1.1/creator'
  |  AND T3.predicate='http://localhost/vocabulary/bench/booktitle'
  |  AND T4.predicate='http://purl.org/dc/terms/partOf'
  |  AND T5.predicate='http://www.w3.org/2000/01/rdf-schema#seeAlso'
  |  AND T6.predicate= 'http://swrc.ontoware.org/ontology#pages'
  |  AND T7.predicate='http://purl.org/dc/elements/1.1/title'
  |  AND T8.predicate='http://xmlns.com/foaf/0.1/homepage'
  |  AND T9.predicate='http://purl.org/dc/terms/issued'
  |
  |  ORDER BY issued
  |
  |
""".stripMargin



  //Q3  6,879  100%


  val q3=
  """
    | SELECT DISTINCT A1.subject AS article
    |  FROM SingleStmtTable A1
    |  LEFT JOIN SingleStmtTable A2 ON A2.subject=A1.subject  AND  A2.predicate= 'http://swrc.ontoware.org/ontology#pages'
    |  WHERE
    |  A1.object='http://localhost/vocabulary/bench/Article'
    |  AND
    |  A2.object IS NOT NULL
  """.stripMargin





  //Q4  100%


  val q4=
    """
      |SELECT DISTINCT
      |    T3.object AS name1, T8.object AS name2
      |    FROM
      |  SingleStmtTable T1 , SingleStmtTable T2 , SingleStmtTable T3,
      |  SingleStmtTable T4, SingleStmtTable T5, SingleStmtTable T6, SingleStmtTable T7, SingleStmtTable T8
      |
      |  WHERE
      |  T1.subject=T2.subject
      |  AND   T2.object=T3.subject
      |  AND   T1.subject=T4.subject
      |  AND   T4.object=T5.object
      |  AND   T5.subject=T6.subject
      |  AND   T6.subject=T7.subject
      |  AND   T7.object=T8.subject
      |
      |
      |  AND
      |  T1.object='http://localhost/vocabulary/bench/Article'
      |  AND T6.object='http://localhost/vocabulary/bench/Article'
      |
      |  AND T2.predicate='http://purl.org/dc/elements/1.1/creator'
      |  AND T7.predicate='http://purl.org/dc/elements/1.1/creator'
      |
      |  AND T3.predicate='http://xmlns.com/foaf/0.1/name'
      |  AND T8.predicate='http://xmlns.com/foaf/0.1/name'
      |
      |  AND T4.predicate='http://swrc.ontoware.org/ontology#journal'
      |  AND T5.predicate='http://swrc.ontoware.org/ontology#journal'
      |
      |  AND T3.object<T8.object
    """.stripMargin





  // Q5  2,383  100%

  val q5=
    """
      |SELECT DISTINCT
      |    T3.subject AS person, T6.object AS name
      |    FROM
      |  SingleStmtTable T1 , SingleStmtTable T2 , SingleStmtTable T3,
      |  SingleStmtTable T4, SingleStmtTable T5, SingleStmtTable T6
      |
      |  WHERE
      |  T1.subject=T2.subject
      |  AND   T2.object=T3.subject
      |
      |  AND   T4.subject=T5.subject
      |  AND   T5.object=T6.subject
      |  AND
      |  T1.object='http://localhost/vocabulary/bench/Article'
      |  AND T4.object='http://localhost/vocabulary/bench/Inproceedings'
      |
      |  AND T2.predicate='http://purl.org/dc/elements/1.1/creator'
      |  AND T5.predicate='http://purl.org/dc/elements/1.1/creator'
      |
      |  AND T3.predicate='http://xmlns.com/foaf/0.1/name'
      |  AND T6.predicate='http://xmlns.com/foaf/0.1/name'
      |  AND T3.object=T6.object
      |
    """.stripMargin





  //Q6  4050

val q6=

  """
    |SELECT
    |  L1.yr       AS yr,
    |  L1.name     AS name,
    |  L1.document AS document
    |  FROM
    |  (
    |    SELECT
    |      T1.subject    AS class,
    |  T2.subject    AS document,
    |  T3.object     AS yr,
    |  T4.object     AS author,
    |  T5.object     AS name
    |    FROM
    |  SingleStmtTable T1
    |
    |  JOIN SingleStmtTable T2     ON T1.subject=T2.object
    |
    |  JOIN SingleStmtTable T3     ON T3.subject=T2.subject
    |
    |  JOIN SingleStmtTable T4     ON T4.subject=T3.subject
    |
    |  JOIN SingleStmtTable T5     ON T5.subject=T4.object
    |
    |  WHERE
    |  T1.predicate='http://www.w3.org/2000/01/rdf-schema#subClassOf'
    |  AND T2.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
    |  AND T3.predicate='http://purl.org/dc/terms/issued'
    |  AND T4.predicate='http://purl.org/dc/elements/1.1/creator'
    |  AND T5.predicate='http://xmlns.com/foaf/0.1/name'
    |  AND T1.object='http://xmlns.com/foaf/0.1/Document'
    |  ) L1
    |
    |  LEFT JOIN
    |    (
    |      SELECT
    |        T1.subject    AS class,
    |  T2.subject    AS document,
    |  T3.object     AS yr,
    |  T4.object     AS author
    |    FROM
    |  SingleStmtTable T1
    |
    |  JOIN SingleStmtTable T2     ON T1.subject=T2.object
    |
    |  JOIN SingleStmtTable T3     ON T3.subject=T2.subject
    |
    |  JOIN SingleStmtTable T4     ON T4.subject=T3.subject
    |
    |  WHERE
    |  T1.predicate='http://www.w3.org/2000/01/rdf-schema#subClassOf'
    |  AND T2.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
    |  AND T3.predicate='http://purl.org/dc/terms/issued'
    |  AND T4.predicate='http://purl.org/dc/elements/1.1/creator'
    |  AND T1.object='http://xmlns.com/foaf/0.1/Document'
    |  ) L2
    |  ON L1.author=L2.author AND L2.yr<L1.yr
    |  WHERE L2.author IS NULL
    |
  """.stripMargin




  //Q7    100%


  val q7=

    """
      |SELECT DISTINCT
      |    title
      |  FROM
      |  (
      |    SELECT
      |      T1.subject AS class,
      |  T2.subject AS doc,
      |  T3.object  AS title,
      |  T5.subject AS doc2,
      |  T5.object  AS bag2
      |    FROM
      |  SingleStmtTable T1
      |    JOIN SingleStmtTable T2     ON T2.object=T1.subject
      |  JOIN SingleStmtTable T3     ON T3.subject=T2.subject
      |  JOIN SingleStmtTable T4     ON T4.object=T3.subject
      |  JOIN SingleStmtTable T5     ON T5.object=T4.subject
      |  WHERE
      |  T1.predicate='http://www.w3.org/2000/01/rdf-schema#subClassOf'
      |  AND T2.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
      |  AND T3.predicate='http://purl.org/dc/elements/1.1/title'
      |  AND T5.predicate= 'http://purl.org/dc/terms/references'
      |  AND T1.object='http://xmlns.com/foaf/0.1/Document'
      |  ) S1
      |  LEFT JOIN
      |    (
      |      SELECT
      |        T6.subject AS class3,
      |  T7.subject AS doc3,
      |  T8.object  AS bag3,
      |  T9.object  AS join1
      |    FROM
      |  SingleStmtTable T6
      |
      |  JOIN SingleStmtTable T7     ON T7.object=T6.subject
      |  JOIN SingleStmtTable T8     ON T8.subject=T7.subject
      |
      |  JOIN SingleStmtTable T9     ON T9.subject=T8.object
      |  LEFT JOIN
      |    (
      |      SELECT
      |        T10.subject AS class4,
      |  T11.subject AS doc4,
      |  T12.object  AS bag4,
      |  T13.object  AS join2
      |    FROM
      |  SingleStmtTable T10
      |
      |
      |
      |  JOIN SingleStmtTable T11     ON T11.object=T10.subject
      |
      |  JOIN SingleStmtTable T12     ON T12.subject=T11.subject
      |
      |  JOIN SingleStmtTable T13     ON T13.subject=T12.object
      |  WHERE
      |  T10.predicate='http://www.w3.org/2000/01/rdf-schema#subClassOf'
      |  AND T11.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
      |  AND T12.predicate= 'http://purl.org/dc/terms/references'
      |  AND T10.object='http://xmlns.com/foaf/0.1/Document'
      |  ) S3 ON T7.subject=S3.join2
      |  WHERE
      |  T6.predicate='http://www.w3.org/2000/01/rdf-schema#subClassOf'
      |  AND T7.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
      |  AND T8.predicate= 'http://purl.org/dc/terms/references'
      |  AND T6.object='http://xmlns.com/foaf/0.1/Document'
      |  AND doc4 IS NULL
      |  ) S2 ON doc=S2.join1
      |  WHERE doc3 IS NULL
      |
      |
    """.stripMargin



  //Q8  301    100%

  val q8=
    """
      |SELECT DISTINCT
      |    name
      |  FROM
      |  SingleStmtTable T1
      |    JOIN SingleStmtTable T2     ON T1.subject=T2.subject
      |  JOIN
      |  (
      |    SELECT
      |      name,
      |    erdoes
      |      FROM
      |      (
      |        SELECT
      |          T5.object     AS name,
      |  T3.object    AS erdoes
      |    FROM
      |  SingleStmtTable T3
      |    JOIN SingleStmtTable T4     ON T3.subject=T4.subject
      |  JOIN SingleStmtTable T5     ON T4.object=T5.subject
      |
      |  WHERE
      |  T3.predicate='http://purl.org/dc/elements/1.1/creator'
      |  AND T4.predicate='http://purl.org/dc/elements/1.1/creator'
      |  AND T5.predicate='http://xmlns.com/foaf/0.1/name'
      |  AND NOT T3.object=T4.object
      |  ) L
      |  UNION
      |  (
      |    SELECT
      |      T7.object AS name,
      |  T3.object AS erdoes
      |    FROM
      |  SingleStmtTable T3
      |    JOIN SingleStmtTable T4     ON T3.subject=T4.subject
      |  JOIN SingleStmtTable T5     ON T4.object=T5.object
      |  JOIN SingleStmtTable T6     ON T5.subject=T6.subject
      |  JOIN SingleStmtTable T7     ON T6.object=T7.subject
      |
      |  WHERE
      |  T3.predicate='http://purl.org/dc/elements/1.1/creator'
      |  AND T4.predicate='http://purl.org/dc/elements/1.1/creator'
      |  AND T5.predicate='http://purl.org/dc/elements/1.1/creator'
      |  AND T6.predicate='http://purl.org/dc/elements/1.1/creator'
      |  AND T7.predicate='http://xmlns.com/foaf/0.1/name'
      |
      |
      |  AND NOT T4.object=T3.object
      |  AND NOT T5.subject=T3.subject
      |  AND NOT T6.object=T3.object
      |  AND NOT T4.object=T6.object
      |  )
      |  ) R ON T2.subject=R.erdoes
      |  WHERE
      |  T1.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
      |  AND T2.predicate='http://xmlns.com/foaf/0.1/name'
      |  AND T1.object='http://xmlns.com/foaf/0.1/Person'
      |  AND T2.object='Paul Erdoes'
      |
    """.stripMargin




  //Q9  4   100%

  val q9=
    """
      |SELECT DISTINCT L.predicate AS predicate
      |  FROM
      |  (
      |    SELECT
      |      T1.subject  AS subject,
      |  T2.predicate AS predicate
      |  FROM
      |  SingleStmtTable T1
      |    JOIN SingleStmtTable T2     ON T1.subject=T2.object
      |
      |  WHERE
      |  T1.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
      |  AND T1.object='http://xmlns.com/foaf/0.1/Person'
      |  UNION
      |  SELECT
      |  T1.subject   AS subject,
      |  T2.predicate AS predicate
      |  FROM
      |  SingleStmtTable T1
      |    JOIN SingleStmtTable T2     ON T1.subject=T2.subject
      |  WHERE
      |  T1.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
      |  AND T1.object='http://xmlns.com/foaf/0.1/Person'
      |  ) L
      |
    """.stripMargin

  //Q10   368   100%

  val q10=
    """
      |SELECT
      |  T.subject  AS subject,
      |  T.predicate AS predicate
      |  FROM
      |  SingleStmtTable T
      |    WHERE
      |  T.object='http://localhost/persons/Paul_Erdoes'
      |
    """.stripMargin




  //Q11    100%  //OFFSET not supported by Spark-SQL

  val q11=
    """
      |
      |  SELECT
      |  T1.object AS ee
      |    FROM SingleStmtTable T1
      |  WHERE T1.predicate='http://www.w3.org/2000/01/rdf-schema#seeAlso'
      |  ORDER BY ee
      |  --OFFSET 50
      |  LIMIT 10
    """.stripMargin






}
