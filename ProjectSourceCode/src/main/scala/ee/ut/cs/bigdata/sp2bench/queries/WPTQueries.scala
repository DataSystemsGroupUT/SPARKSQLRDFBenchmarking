package ee.ut.cs.bigdata.sp2bench.queries

class WPTQueries {


  val q1 =
    """
    	|SELECT issued as year
    	|FROM WPT
    	|WHERE
    	|type="http://localhost/vocabulary/bench/Journal" AND title="Journal 1 (1940)"
  	""".stripMargin


  val q2 =
    """
    	|SELECT
    	|	Subject AS inproc,
    	|	creator AS author,
    	|	booktitle,
    	|	title,
    	|	seeAlso,
    	|	pages,
    	|	homepage  AS url,
    	|	issued	AS yr,
    	|	abstract
    	|
    	|FROM WPT
    	|
    	|WHERE
    	|	type='http://localhost/vocabulary/bench/Inproceedings'
    	|	AND booktitle IS NOT NULL
    	|	AND creator IS NOT NULL
    	|	AND partOf IS NOT NULL
    	|	AND seeAlso IS NOT NULL
    	|	AND title IS NOT NULL
    	|	AND pages IS NOT NULL
    	|	AND homepage IS NOT NULL
    	|	AND issued IS NOT NULL
    	|ORDER BY yr
  	""".stripMargin


  val q3 =
    """
    	|SELECT Distinct Subject AS article
    	|FROM WPT
    	|WHERE
    	|type='http://localhost/vocabulary/bench/Article'
    	|AND pages IS NOT NULL
        """.stripMargin


  val q4 =
    """
    	|SELECT DISTINCT
    	|	WPT2.name AS name1,
    	|	WPT4.name AS name2
    	|FROM WPT WPT1
    	|JOIN WPT WPT2 ON WPT1.creator=WPT2.Subject
    	|JOIN WPT WPT3 ON WPT1.journal=WPT3.journal
    	|JOIN WPT WPT4 ON WPT3.creator=WPT4.Subject
    	|WHERE
    	|WPT1.type='http://localhost/vocabulary/bench/Article'
    	|AND WPT3.type='http://localhost/vocabulary/bench/Article'
    	| AND WPT2.name<WPT4.name
        """.stripMargin


  val q5 =
    """
    	|SELECT DISTINCT
    	|	Pe1.Subject AS person,
    	|	Pe1.name AS name
    	|FROM
    	|	WPT WPT1
    	|	JOIN WPT WPT2
    	|	ON WPT1.creator=WPT2.creator
    	|	AND WPT1.type="http://localhost/vocabulary/bench/Article"
    	|	AND WPT2.type="http://localhost/vocabulary/bench/Inproceedings"
    	|	JOIN WPT Pe1 ON WPT1.creator=Pe1.Subject
    	|	JOIN WPT Pe2 ON WPT2.creator=Pe2.Subject
    	|	WHERE
    	|	Pe1.name=Pe2.name
        """.stripMargin


  val q6 =
    """
    	|SELECT
    	|	P.issued  AS yr,
    	|	A.name  AS name,
    	|	P.Subject AS document
    	|FROM
    	|	WPT P
    	|	JOIN WPT A
    	|	ON P.creator=A.Subject
    	|WHERE
    	|	NOT EXISTS (
    	|    	SELECT *
    	|    	FROM
    	|        	WPT P2
    	|        	JOIN WPT A2
    	|        	ON P2.creator=A2.Subject
    	|    	WHERE
    	|        	A.Subject=A2.Subject
    	|        	AND P2.issued<P.issued
    	|	) AND P.issued IS NOT NULL
        """.stripMargin


  val q8 =
    """
    	|SELECT DISTINCT name
    	|FROM (
    	|(
    	|	SELECT
    	|    	Pe4.name AS name
    	|	FROM
    	|        	WPT P1
    	|        	JOIN WPT Pe1 ON P1.creator=Pe1.Subject
    	|        	JOIN WPT P2  ON P1.Subject=P2.Subject
    	|        	JOIN WPT Pe2 ON P2.creator=Pe2.Subject
    	|        	JOIN WPT P3  ON P3.creator=Pe2.Subject
    	|        	JOIN WPT P4  ON P3.Subject=P4.Subject
    	|        	JOIN WPT Pe4 ON P4.creator=Pe4.Subject
    	|	WHERE
    	|    	Pe1.name='Paul Erdoes'
    	|    	AND NOT Pe4.name='Paul Erdoes'
    	|)
    	|UNION
    	|(
    	|   	SELECT
    	|   	Pe2.name AS name
    	|       	FROM
    	|        	WPT P1
    	|           	JOIN WPT P2  ON P1.Subject=P2.Subject
    	|           	JOIN WPT Pe1 ON P1.creator=Pe1.Subject
    	|           	JOIN WPT Pe2 ON P2.creator=Pe2.Subject
    	|       	WHERE
    	|            	Pe1.name='Paul Erdoes'
    	|    	AND NOT Pe2.name='Paul Erdoes'
    	|)) AS dist
        """.stripMargin


  val q10 =
    """
  	|SELECT
  	|	P.Subject   AS subject,
  	|	'dc:creator' AS predicate
  	|FROM
  	|	WPT P
  	|	JOIN WPT   A   	ON P.creator=A.Subject
  	|WHERE
  	|	A.name='Paul Erdoes'
  	|UNION
  	|SELECT
  	|	E.Subject	AS subject,
  	|	'swrc:editor' AS predicate
  	|FROM
  	|	WPT E
  	|	JOIN WPT P ON E.editor=P.Subject
  	|WHERE
  	|	P.name='Paul Erdoes'
        """.stripMargin


  val q11 =
    """
    	|SELECT seeAlso
    	|FROM
    	|WPT
    	|WHERE seeAlso IS NOT NULL
    	|ORDER BY seeAlso
    	|LIMIT 10
        """.stripMargin

}
