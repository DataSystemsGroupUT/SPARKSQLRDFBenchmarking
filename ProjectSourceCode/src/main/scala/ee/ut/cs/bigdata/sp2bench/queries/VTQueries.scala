package ee.ut.cs.bigdata.sp2bench.queries

class VTQueries {




  //Q1  100%

val q1=
  """
    |SELECT
    |  DI.object AS yr
    |FROM
    |    type RT
    |    JOIN title DT       ON RT.subject=DT.subject
    |    JOIN issued DI      ON RT.subject=DI.subject
    |WHERE
    |    RT.object='http://localhost/vocabulary/bench/Journal'
    |    AND DT.object='Journal 1 (1940)'
  """.stripMargin


  // Q2    2,149   xrH3b6b  100%


val q2=
"""
  |SELECT
  |    DP.subject  AS inproc,
  |    DC.subject  AS author,
  |    BB.object   AS booktitle,
  |    DT.object   AS title,
  |    DP.object   AS proc,
  |    RS.object   AS ee,
  |    SP.object   AS page,
  |    FH.object   AS URL,
  |    DI.object  AS yr,
  |    BA.object  AS abstract
  |FROM
  |    type RT
  |    JOIN creator DC      ON RT.subject=DC.subject
  |    JOIN booktitle BB ON RT.subject=BB.subject
  |    JOIN title DT        ON RT.subject=DT.subject
  |    JOIN partOf DP  ON RT.subject=DP.subject
  |    JOIN seeAlso RS    ON RT.subject=RS.subject
  |    JOIN pages SP      ON RT.subject=SP.subject
  |    JOIN homepage FH   ON RT.subject=FH.subject
  |    JOIN issued DI  ON RT.subject=DI.subject
  |    LEFT JOIN abstractv BA ON RT.subject=BA.subject
  |ORDER BY DI.object
""".stripMargin



  //Q3  6,879  100%


  val q3=
  """
    | SELECT DISTINCT Ty.subject AS article
    |
    |FROM Type Ty
    |JOIN Pages P ON Ty.subject=P.subject
    |WHERE
    |Ty.object='http://localhost/vocabulary/bench/Article'
    |AND
    |P.object IS NOT NULL
  """.stripMargin





  //Q4  100%


  val q4=
    """
      |SELECT DISTINCT
      |    N1.object AS name1,
      |    N2.object AS name2
      |FROM
      |    Type A1
      |    JOIN Creator C1 ON A1.subject=C1.subject
      |    JOIN Name N1    ON C1.object=N1.subject
      |    JOIN Journal J1 ON A1.subject=J1.subject
      |    JOIN Journal J2 ON J1.object=J2.object
      |    JOIN Type A2    ON A2.subject=J2.subject
      |    JOIN Creator C2 ON A2.subject=C2.subject
      |    JOIN Name N2    ON C2.object=N2.subject
      |WHERE
      |    A1.object='http://localhost/vocabulary/bench/Article'
      |    AND
      |    A2.object='http://localhost/vocabulary/bench/Article'
      |    AND N1.object<>N2.object
    """.stripMargin





  // Q5  2,383  100%

  val q5=
    """
      |SELECT DISTINCT
      |    N1.subject AS person,
      |    N1.object AS name
      |FROM
      |    Type P1
      |    JOIN Creator C1 ON P1.subject=C1.subject
      |    JOIN Name N1    ON C1.object=N1.subject,
      |    Type P2
      |    JOIN Creator C2 ON P2.subject=C2.subject
      |    JOIN Name N2    ON C2.object=N2.subject
      |WHERE
      |    P1.object='http://localhost/vocabulary/bench/Article'  AND
      |    P2.object='http://localhost/vocabulary/bench/Inproceedings'
      |    AND N1.object=N2.object
    """.stripMargin





  //Q6  4050

val q6=

  """
    |SELECT DISTINCT
    |    L1.yr       AS yr,
    |    L1.name     AS name,
    |    L1.document AS document
    |FROM
    |    (
    |        SELECT
    |            RT1.subject    AS class,
    |            RT2.subject    AS document,
    |            DI.object      AS yr,
    |            DC.object      AS author,
    |            FN.object      AS name
    |        FROM
    |            subClassOf   RT1
    |            JOIN Type RT2      ON RT1.subject=RT2.object
    |            JOIN Issued DI ON DI.subject=RT2.subject
    |            JOIN Creator DC     ON DC.subject=DI.subject
    |            JOIN Name FN      ON DC.object=FN.subject
    |        WHERE RT1.object='http://xmlns.com/foaf/0.1/Document'
    |    ) AS L1
    |
    |    LEFT JOIN
    |    (
    |        SELECT
    |			RT1.subject AS class,
    |            RT2.subject      AS document,
    |            DI.object      AS yr,
    |            DC.object   AS author
    |
    |        FROM
    |            subClassOf RT1
    |            JOIN Type RT2      ON RT1.subject=RT2.object
    |            JOIN Issued DI ON DI.subject=RT2.subject
    |            JOIN Creator DC     ON DC.subject=DI.subject
    |
    |        WHERE RT1.object='http://xmlns.com/foaf/0.1/Document'
    |
    |    ) AS L2
    |    ON L1.author=L2.author AND L2.yr<L1.yr
    |WHERE L2.author IS NULL
  """.stripMargin




  //Q7    100%


  val q7=

    """
      |SELECT DISTINCT
      |    title
      |FROM
      |    (
      |
      |        SELECT
      |            RT1.subject AS class,
      |            RT2.subject AS doc,
      |            DT.object      AS title,
      |            DR.subject  AS doc2,
      |            DR.object   AS bag2
      |        FROM
      |            subClassOf RT1
      |
      |            JOIN type RT2          ON RT2.object=RT1.subject
      |            JOIN title DT           ON DT.subject=RT2.subject
      |            JOIN reference T ON T.cited=DT.subject
      |            JOIN referencesv DR ON DR.subject=T.document
      |        WHERE
      |            RT1.object='http://xmlns.com/foaf/0.1/Document'
      |
      |    ) AS S1
      |
      |    LEFT JOIN
      |    (
      |        SELECT
      |            RT3.subject AS class3,
      |            RT4.subject AS doc3,
      |            DR2.object  AS bag3,
      |            T2.cited   AS join1
      |        FROM
      |        (
      |            subClassOf RT3
      |            JOIN type RT4           ON RT4.object=RT3.subject
      |            JOIN referencesv DR2 ON DR2.subject=RT4.subject
      |            JOIN reference T2 ON T2.document=DR2.subject
      |            LEFT JOIN
      |            (
      |                SELECT
      |                    RT5.subject AS class4,
      |                    RT6.subject AS doc4,
      |                    DR3.object  AS bag4,
      |                    T3.cited   AS join2
      |                FROM
      |                    subClassOf RT5
      |                    JOIN type RT6           ON RT6.object=RT5.subject
      |                    JOIN referencesv DR3 ON DR3.subject=RT6.subject
      |                    JOIN reference T3 ON  T3.document=DR3.subject
      |                WHERE
      |                    RT5.object='http://xmlns.com/foaf/0.1/Document'
      |            ) AS S3 ON RT4.subject=S3.join2
      |        )
      |        WHERE
      |            RT3.object='http://xmlns.com/foaf/0.1/Document'
      |            AND doc4 IS NULL
      |    ) AS S2 ON doc=S2.join1
      |WHERE doc3 IS NULL
    """.stripMargin



  //Q8  301    100%

  val q8=
    """
      |SELECT DISTINCT
      |    name
      |FROM
      |    Type RT
      |    JOIN Name FN  ON RT.subject=FN.subject
      |    JOIN
      |    (
      |        SELECT
      |            name,
      |            erdoes
      |        FROM
      |        (
      |            SELECT
      |                FN2.object AS name,
      |                DC1.object AS erdoes
      |            FROM
      |                Creator DC1
      |                JOIN Creator DC2 ON DC1.subject=DC2.subject
      |                JOIN Name FN2  ON DC2.object=FN2.subject
      |            WHERE
      |                NOT DC1.object=DC2.object
      |        ) AS L
      |        UNION
      |        (
      |            SELECT
      |                FN2.object AS name,
      |                DC1.object AS erdoes
      |            FROM
      |                Creator DC1
      |                JOIN Creator DC2 ON DC1.subject=DC2.subject
      |                JOIN Creator DC3 ON DC2.object=DC3.object
      |                JOIN Creator DC4 ON DC3.subject=DC4.subject
      |                JOIN Name FN2  ON DC4.object=FN2.subject
      |            WHERE
      |                NOT DC2.object=DC1.object
      |                AND NOT DC3.subject=DC1.subject
      |                AND NOT DC4.object=DC1.object
      |                AND NOT DC2.object=DC4.object
      |        )
      |    ) AS R ON FN.subject=R.erdoes
      |WHERE
      |    RT.object='http://xmlns.com/foaf/0.1/Person'
      |    AND FN.object='Paul Erdoes'
    """.stripMargin




  //Q9  4   100%

  val q9=
    """
      |SELECT DISTINCT predicate
      |FROM
      |    (
      |        --#START_UNION#
      |        SELECT
      |            RT.subject,
      |            T.predicate
      |        FROM
      |            type RT
      |            JOIN SingleStmtTable AS T ON RT.subject=T.object
      |        WHERE
      |            RT.object='http://xmlns.com/foaf/0.1/Person'
      |        --#END_UNION#
      |        UNION
      |        --#START_UNION#
      |        SELECT
      |            RT.subject,
      |            T.predicate
      |        FROM
      |            type RT
      |            JOIN SingleStmtTable AS T ON RT.subject=T.subject
      |        WHERE
      |            RT.object='http://xmlns.com/foaf/0.1/Person'
      |        --#END_UNION#
      |    ) AS L (subject,predicate) where L.predicate not like 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_%'
    """.stripMargin

  //Q10   368   100%

  val q10=
    """
      |SELECT
      |    L.subject AS subject, L.predicate AS predicate
      |FROM
      |(
      |SELECT A.subject, "dc:#Creator" As predicate  FROM Creator A WHERE  A.object='http://localhost/persons/Paul_Erdoes'
      |UNION
      |SELECT E.subject , "dc:#Editor" As predicate  FROM Editorv E  WHERE  E.object='http://localhost/persons/Paul_Erdoes'
      |) AS L
    """.stripMargin




  //Q11    100%  //OFFSET not supported by Spark-SQL

  val q11=
    """
      |SELECT
      |    RSA.object AS ee
      |FROM
      |    SeeAlso RSA
      |
      |ORDER BY ee
      |--OFFSET 50
      |LIMIT 10
    """.stripMargin





}
