package ee.ut.cs.bigdata.sp2bench.queries

class PTQueries {



val q1=
  """
    |SELECT
    |    D.issued AS yr
    |FROM
    |    Venue V
    |    JOIN Document D   ON D.document=V.Venue
    |    JOIN VenueType VT ON VT.document=V.Venue
    |WHERE
    |    VT.type='http://localhost/vocabulary/bench/Journal'
    |    AND D.title='Journal 1 (1940)'
  """.stripMargin




val q2=
"""
  |SELECT
  |    D1.document  AS inproc,
  |    Pe.name      AS author,
  |    D1.booktitle AS booktitle,
  |    D1.title     AS title,
  |    V.title     AS proc,
  |    DSA.seeAlso  AS ee,
  |    P.pages      AS page,
  |    DH.homepage  AS url,
  |    D1.issued    AS yr,
  |    AB.txt       AS abstract
  |FROM
  |    Document D1
  |    JOIN Publication P          ON P.publication=D1.document
  |    JOIN PublicationType PT     ON P.publication=PT.publication
  |    JOIN Author A               ON P.publication=A.document
  |    JOIN Person Pe              ON Pe.person=A.person
  |    JOIN Document_seeAlso DSA   ON DSA.document=D1.document
  |    JOIN Document_homepage DH   ON DH.document=D1.document
  |
  |     JOIN Venue V                ON P.venue=V.Venue
  |
  |    --JOIN Document D2            ON D2.ID=V.fk_document
  |
  |    LEFT OUTER JOIN Abstract AB ON AB.publication=P.publication
  |WHERE
  |    PT.type='http://localhost/vocabulary/bench/Inproceedings'
  |    AND D1.booktitle IS NOT NULL
  |    AND D1.title IS NOT NULL
  |    AND P.pages IS NOT NULL
  |    AND D1.issued IS NOT NULL
  |ORDER BY yr
""".stripMargin






  val q3=
  """
    | SELECT
    |    D.document AS article
    |FROM
    |    Publication P
    |    JOIN Document D         ON D.document=P.publication
    |    JOIN PublicationType PT ON P.publication=PT.publication
    |WHERE
    |    PT.type='http://localhost/vocabulary/bench/Article'
    |    AND P.pages IS NOT NULL
  """.stripMargin








  val q4=
    """
      |SELECT DISTINCT
      |    Pe1.name AS name1,
      |    Pe2.name AS name2
      |FROM
      |    Publication P1
      |    JOIN PublicationType PT1 ON P1.publication=PT1.publication
      |    JOIN Author A1           ON A1.document=P1.publication
      |    JOIN Person Pe1          ON A1.person=Pe1.person
      |
      |    JOIN Venue V             ON P1.venue=V.Venue
      |    JOIN Publication P2      ON P2.venue=V.Venue
      |
      |    JOIN PublicationType PT2 ON P2.publication=PT2.publication
      |    JOIN Author A2           ON A2.document=P2.publication
      |    JOIN Person Pe2          ON A2.person=Pe2.person
      |WHERE
      |    PT1.type='http://localhost/vocabulary/bench/Article'
      |    AND PT2.type='http://localhost/vocabulary/bench/Article'
      |    AND Pe1.name<Pe2.name
    """.stripMargin






  val q5=
    """
      |SELECT DISTINCT
      |    Pe1.person AS person,
      |    Pe1.name     AS name
      |FROM
      |    Publication P1
      |    JOIN PublicationType PT1 ON P1.publication=PT1.publication
      |    JOIN Author A1           ON P1.publication=A1.document
      |    JOIN Person Pe1          ON A1.person=Pe1.person,
      |    Publication P2
      |    JOIN PublicationType PT2 ON P2.publication=PT2.publication
      |    JOIN Author A2           ON P2.publication=A2.document
      |    JOIN Person Pe2          ON A2.person=Pe2.person
      |WHERE
      |    PT1.type='http://localhost/vocabulary/bench/Article'
      |    AND PT2.type='http://localhost/vocabulary/bench/Inproceedings'
      |    AND Pe1.name=Pe2.name
    """.stripMargin





val q6=

  """
    |SELECT
    |    D.issued   AS yr,
    |    Pe.name    AS name,
    |    D.document AS document
    |FROM
    |    Publication P
    |    JOIN Document D ON D.document=P.publication
    |    JOIN Author A   ON P.publication=A.document
    |    JOIN Person Pe  ON Pe.person=A.person
    |WHERE
    |    NOT EXISTS (
    |        SELECT *
    |        FROM
    |            Publication P2
    |            JOIN Document D2 ON D2.document=P2.publication
    |            JOIN Author A2   ON P2.publication=A2.document
    |        WHERE
    |            A.person=A2.person
    |            AND D2.issued<D.issued
    |    ) AND D.issued IS NOT NULL
  """.stripMargin



  val q7=

    """
      |SELECT DISTINCT
      |    D.title AS title
      |FROM
      |    Publication P
      |    JOIN Document D  ON D.document=P.publication
      |    JOIN Reference R ON P.publication=R.cited
      |WHERE
      |    P.publication NOT IN (
      |        SELECT cited
      |        FROM Reference R2
      |        WHERE R2.document NOT IN (
      |            SELECT cited FROM Reference R3
      |        )
      |    )
    """.stripMargin




  val q8=
    """
      |SELECT DISTINCT name
      |FROM (
      |(
      |    SELECT
      |        Pe4.name AS name
      |    FROM
      |        Author A1
      |        JOIN Person Pe1 ON A1.person=Pe1.person
      |        JOIN Author A2  ON A1.document=A2.document
      |        JOIN Author A3  ON A2.person=A3.person
      |        JOIN Author A4  ON A3.document=A4.document
      |        JOIN Person Pe4 ON A4.person=Pe4.person
      |    WHERE
      |        Pe1.name='Paul Erdoes'
      |        AND NOT Pe4.name='Paul Erdoes'
      |)
      |UNION
      |(
      |    SELECT
      |        Pe2.name AS name
      |    FROM
      |        Author A1
      |        JOIN Person Pe1 ON A1.person=Pe1.person
      |        JOIN Author A2  ON A1.document=A2.document
      |        JOIN Person Pe2 ON A2.person=Pe2.person
      |    WHERE
      |        Pe1.name='Paul Erdoes'
      |        AND NOT Pe2.name='Paul Erdoes'
      |)) AS dist
    """.stripMargin




  //Q9 NA



  val q10=
    """
      |SELECT
      |    D.document   AS subject,
      |    'dc:creator' AS predicate
      |FROM
      |    Author A
      |    JOIN Publication P ON A.document=P.publication
      |    JOIN Document D    ON D.document=P.publication
      |    JOIN Person Pe     ON A.person=Pe.person
      |WHERE
      |    Pe.name='Paul Erdoes'
      |UNION
      |SELECT
      |    D.document    AS subject,
      |    'swrc:editor' AS predicate
      |FROM
      |    Editor E
      |    JOIN Document D ON D.document=E.document
      |
      |    JOIN Person Pe  ON E.person=Pe.person
      |WHERE
      |    Pe.name='Paul Erdoes'
      |
    """.stripMargin




  //Q11     //OFFSET not supported by Spark-SQL

  val q11=
    """
      | SELECT
      |    DSA.seeAlso
      |FROM
      |    Publication P
      |    JOIN Document D ON D.document=P.publication
      |    JOIN Document_seeAlso DSA ON DSA.document=D.document
      |ORDER BY DSA.seeAlso
      |LIMIT 10
    """.stripMargin






}
