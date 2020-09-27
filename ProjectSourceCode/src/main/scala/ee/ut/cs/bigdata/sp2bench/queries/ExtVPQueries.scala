package ee.ut.cs.bigdata.sp2bench.queries

class ExtVPQueries {



val q1=
  """
    SELECT tab3.Object FROM  
    (SELECT Subject , Object 
      FROM ExtVP_SS_Type_Issued 
      WHERE Object ='http://localhost/vocabulary/bench/Journal'
    ) tab1 
    JOIN 
    (SELECT Subject , Object 
    FROM ExtVP_SS_Title_Issued 
    WHERE Object='Journal 1 (1940)'
    ) tab2 
    ON (tab1.Subject=tab2.Subject) 
    JOIN 
    (SELECT Subject , Object 
    FROM VP_Issued
    ) tab3 
    ON (tab1.Subject=tab3.Subject)
  """.stripMargin


val q2=
      """
        |SELECT tab16.ee AS ee , tab16.proc AS proc , tab16.inproc AS inproc , tab16.author AS author , tab16.yr AS yr , tab16.page AS page , tab17.abstract AS abstract , tab16.booktitle AS booktitle , tab16.title AS title , tab16.url AS url
        | FROM    (SELECT tab5.ee AS ee , tab4.proc AS proc , tab0.inproc AS inproc , tab1.author AS author , tab8.yr AS yr , tab6.page AS page , tab2.booktitle AS booktitle , tab3.title AS title , tab7.url AS url
        | FROM    (SELECT Subject AS inproc
        |     FROM ExtVP_SS_Type_PartOf
        |     WHERE Object = 'http://localhost/vocabulary/bench/Inproceedings'
        |    ) tab0
        | JOIN    (SELECT Object AS proc , Subject AS inproc
        |     FROM ExtVP_SS_PartOf_SeeAlso
        |
        |    ) tab4
        | ON(tab0.inproc=tab4.inproc)
        | JOIN    (SELECT Object AS ee , Subject AS inproc
        |     FROM ExtVP_SS_SeeAlso_PartOf
        |
        |    ) tab5
        | ON(tab4.inproc=tab5.inproc)
        | JOIN    (SELECT Subject AS inproc , Object AS yr
        |     FROM ExtVP_SS_Issued_SeeAlso
        |
        |    ) tab8
        | ON(tab5.inproc=tab8.inproc)
        | JOIN    (SELECT Subject AS inproc , Object AS booktitle
        |     FROM ExtVP_SS_BookTitle_SeeAlso
        |
        |    ) tab2
        | ON(tab8.inproc=tab2.inproc)
        | JOIN    (SELECT Subject AS inproc , Object AS page
        |     FROM ExtVP_SS_Pages_PartOf
        |    ) tab6
        | ON(tab2.inproc=tab6.inproc)
        | JOIN    (SELECT Subject AS inproc , Object AS title
        |     FROM ExtVP_SS_Title_PartOf
        |    ) tab3
        | ON(tab6.inproc=tab3.inproc)
        | JOIN    (SELECT Subject AS inproc , Object AS url
        |     FROM ExtVP_SS_HomePage_PartOf
        |
        |    ) tab7
        | ON(tab3.inproc=tab7.inproc)
        | JOIN    (SELECT Subject AS inproc , Object AS author
        |     FROM ExtVP_SS_Creator_PartOf
        |    ) tab1
        | ON(tab7.inproc=tab1.inproc)
        |
        |) tab16
        | LEFT OUTER JOIN    (SELECT Subject AS inproc , Object AS abstract
        |     FROM VP_Abstract
        |
        |    ) tab17
        | ON(tab16.inproc=tab17.inproc)
        |
        |  ORDER BY yr
      """.stripMargin



val q3=
          """
            |SELECT DISTINCT tab0.Subject AS article
            | FROM (SELECT Subject  FROM ExtVP_SS_Type_Pages
            |     WHERE Object = 'http://localhost/vocabulary/bench/Article'
            |    ) tab0
            | JOIN    (SELECT Object , Subject
            |     FROM VP_Pages
            |    ) tab1
            | ON(tab0.Subject=tab1.Subject)
          """.stripMargin


val q4=
"""
      |SELECT DISTINCT  tab5.name2 AS name2 , tab3.name1 AS name1
      | FROM    (SELECT Subject AS article1
      |     FROM ExtVP_SS_Type_Journal
      |     WHERE Object = 'http://localhost/vocabulary/bench/Article'
      |    ) tab0
      | JOIN    (SELECT Object AS journal , Subject AS article1
      |     FROM VP_Journal
      |
      |    ) tab6
      | ON(tab0.article1=tab6.article1)
      | JOIN    (SELECT Object AS journal , Subject AS article2
      |     FROM VP_Journal
      |
      |    ) tab7
      | ON(tab6.journal=tab7.journal)
      | JOIN    (SELECT Subject AS article2
      |     FROM ExtVP_SS_Type_Journal
      |     WHERE Object = 'http://localhost/vocabulary/bench/Article'
      |    ) tab1
      | ON(tab7.article2=tab1.article2)
      | JOIN    (SELECT Object AS author1 , Subject AS article1
      |     FROM ExtVP_SS_Creator_Journal
      |    ) tab2
      | ON(tab6.article1=tab2.article1)
      | JOIN    (SELECT Subject AS author1 , Object AS name1
      |     FROM ExtVP_SO_Name_Creator
      |    ) tab3
      | ON(tab2.author1=tab3.author1)
      | JOIN    (SELECT Object AS author2 , Subject AS article2
      |     FROM ExtVP_SS_Creator_Journal
      |    ) tab4
      | ON(tab1.article2=tab4.article2)
      | JOIN    (SELECT Subject AS author2 , Object AS name2
      |     FROM ExtVP_SO_Name_Creator
      |    ) tab5
      | ON(tab4.author2=tab5.author2)
      |
      | WHERE (tab3.name1 < tab5.name2)
    """.stripMargin


val q5=
"""
        |SELECT  DISTINCT tab1.person AS person , tab4.name AS name
        | FROM    (SELECT Subject AS article
        |     FROM ExtVP_SS_Type_Creator
        |     WHERE Object = 'http://localhost/vocabulary/bench/Article'
        |    ) tab0
        | JOIN    (SELECT Object AS person , Subject AS article
        |     FROM VP_Creator
        |    ) tab1
        | ON(tab0.article=tab1.article)
        | JOIN    (SELECT Subject AS person , Object AS name
        |     FROM ExtVP_SO_Name_Creator
        |    ) tab4
        | ON(tab1.person=tab4.person)
        | CROSS JOIN    (SELECT Subject AS inproc
        |     FROM ExtVP_SS_Type_Creator
        |     WHERE Object = 'http://localhost/vocabulary/bench/Inproceedings'
        |    ) tab2
        | JOIN    (SELECT Object AS person2 , Subject AS inproc
        |     FROM VP_Creator
        |    ) tab3
        | ON(tab2.inproc=tab3.inproc)
        | JOIN    (SELECT Subject AS person2 , Object AS name2
        |     FROM ExtVP_SO_Name_Creator
        |    ) tab5
        | ON(tab3.person2=tab5.person2)
        |
        | WHERE (tab4.name = tab5.name2)
      """.stripMargin


val q6=
"""
        |SELECT DISTINCT tab8.document AS document , tab8.name AS name , tab8.yr AS yr
        | FROM    (SELECT tab3.author AS author , tab1.document AS document , tab4.name AS name , tab2.yr AS yr , tab0.class AS class
        | FROM    (SELECT Subject AS class
        |     FROM VP_SubClassOf
        |
        |     WHERE Object = 'http://xmlns.com/foaf/0.1/Document'
        |    ) tab0
        | JOIN    (SELECT Subject AS document , Object AS class
        |     FROM ExtVP_SS_Type_Issued
        |    ) tab1
        | ON(tab0.class=tab1.class)
        | JOIN    (SELECT Subject AS document , Object AS yr
        |     FROM ExtVP_SS_Issued_Creator
        |
        |    ) tab2
        | ON(tab1.document=tab2.document)
        | JOIN    (SELECT Object AS author , Subject AS document
        |     FROM ExtVP_SS_Creator_Issued
        |    ) tab3
        | ON(tab2.document=tab3.document)
        | JOIN    (SELECT Subject AS author , Object AS name
        |     FROM ExtVP_SO_Name_Creator
        |    ) tab4
        | ON(tab3.author=tab4.author)
        |
        |) tab8
        |
        |LEFT JOIN    (SELECT tab12.author2 AS author2 , tab9.class2 AS class2 , tab11.yr2 AS yr2 , tab10.document2 AS document2
        | FROM    (SELECT Subject AS class2
        |     FROM VP_SubClassOf
        |
        |     WHERE Object = 'http://xmlns.com/foaf/0.1/Document'
        |    ) tab9
        | JOIN    (SELECT Object AS class2 , Subject AS document2
        |     FROM ExtVP_SS_Type_Issued
        |    ) tab10
        | ON(tab9.class2=tab10.class2)
        | JOIN    (SELECT Object AS yr2 , Subject AS document2
        |     FROM ExtVP_SS_Issued_Creator
        |
        |    ) tab11
        | ON(tab10.document2=tab11.document2)
        | JOIN    (SELECT Object AS author2 , Subject AS document2
        |     FROM ExtVP_SS_Creator_Issued
        |    ) tab12
        | ON(tab11.document2=tab12.document2)
        |
        |) tab15
        |ON (tab8.author = tab15.author2) AND (tab15.yr2 < tab8.yr)
        |WHERE tab15.author2 IS NULL
       """.stripMargin

val q8=
"""
      |      SELECT DISTINCT  UNION1.name AS name
      |      FROM    (SELECT tab0.erdoes AS erdoes
      |      FROM    (SELECT Subject AS erdoes
      |          FROM ExtVP_SS_Type_Name
      |        WHERE Object = 'http://xmlns.com/foaf/0.1/Person'
      |          ) tab0
      |       JOIN    (SELECT Subject AS erdoes
      |           FROM VP_Name
      |           WHERE Object = "Paul Erdoes"
      |          ) tab1
      |      ON(tab0.erdoes=tab1.erdoes)
      |
      |      ) tab2
      |      JOIN (SELECT tab7.author2 AS author2 , tab5.author AS author , tab4.document AS document , tab7.name AS name , tab3.erdoes AS erdoes , tab6.document2 AS document2
      |      FROM    (SELECT Subject AS author2 , Object AS name
      |          FROM ExtVP_SO_Name_Creator
      |         ) tab7
      |       JOIN    (SELECT Object AS author2 , Subject AS document2
      |           FROM VP_Creator
      |          ) tab6
      |       ON(tab7.author2=tab6.author2)
      |       JOIN    (SELECT Object AS author , Subject AS document2
      |           FROM VP_Creator
      |          ) tab5
      |       ON(tab6.document2=tab5.document2)
      |       JOIN    (SELECT Object AS author , Subject AS document
      |           FROM VP_Creator
      |          ) tab4
      |       ON(tab5.author=tab4.author)
      |       JOIN    (SELECT Subject AS document , Object AS erdoes
      |           FROM VP_Creator
      |          ) tab3
      |       ON(tab4.document=tab3.document)
      |
      |       WHERE ((((tab5.author != tab3.erdoes) AND (tab6.document2 != tab4.document)) AND (tab7.author2 != tab3.erdoes)) AND (tab7.author2 != tab5.author))
      |       UNION ALL SELECT null AS author2 , tab14.author AS author , tab13.document AS document , tab14.name AS name , tab12.erdoes AS erdoes , null AS document2
      |       FROM    (SELECT Subject AS author , Object AS name
      |           FROM ExtVP_SO_Name_Creator
      |          ) tab14
      |       JOIN    (SELECT Object AS author , Subject AS document
      |           FROM VP_Creator
      |          ) tab13
      |       ON(tab14.author=tab13.author)
      |       JOIN    (SELECT Subject AS document , Object AS erdoes
      |           FROM VP_Creator
      |          ) tab12
      |       ON(tab13.document=tab12.document)
      |
      |       WHERE (tab14.author != tab12.erdoes)
      |      ) UNION1
      |       ON(tab2.erdoes=UNION1.erdoes)
      |    """.stripMargin


val q9=
"""
        |select distinct Predicate FROM
        |(
        |  SELECT t1.Subject, t2.Predicate from VP_Type t1
        |  join Triples t2 ON t1.Subject=t2.Object
        |   WHERE t1.Object='http://xmlns.com/foaf/0.1/Person'
        |
        |UNION
        |  SELECT t1.Subject, t2.Predicate from VP_Type t1
        |  join Triples  t2 ON t2.Subject=t1.Subject
        |   WHERE t1.Object='http://xmlns.com/foaf/0.1/Person'
        |)
        |AS L (subject,predicate) where L.predicate not like 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_%'
      """.stripMargin



val q9_old=
"""
        |select distinct Predicate FROM
        |(
        |  SELECT t2.Subject, t1.Predicate from Triples t1
        |  join VP_Type  t2
        |  ON t1.Object=t2.Subject AND  t2.Object='http://xmlns.com/foaf/0.1/Person'
        |
        |UNION
        |  SELECT t2.Subject, t1.Predicate from Triples t1
        |  join VP_Type  t2
        |  ON t1.Subject=t2.Subject AND  t2.Object='http://xmlns.com/foaf/0.1/Person'
        |)
        |AS L (subject,predicate) where L.predicate not like 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_%'
      """.stripMargin

 

val q10=
      """
        |select DISTINCT Subject, Predicate  FROM
        |(
        |
        |  SELECT Subject, "dc:#Editor" AS Predicate from VP_Editor E
        |  WHERE E.Object='http://localhost/persons/Paul_Erdoes'
        |
        |UNION
        |  SELECT Subject, "dc:#Creator" AS Predicate from VP_Creator A
        |  WHERE A.Object='http://localhost/persons/Paul_Erdoes'
        |
        |)
        |AS L
      """.stripMargin

val q11=
      """
        |SELECT Object AS ee
        |FROM VP_SeeAlso
        |ORDER BY ee
        |LIMIT 10
      """.stripMargin


val q10_old=
      """
        |select distinct Subject, Predicate FROM
        |(
        |
        |  SELECT Subject, Predicate from Triples t1
        |  left semi join  VP_Editor t2
        |  ON t1.Object=t2.Object AND  t2.Object='_:b3091'
        |
        |UNION
        |  SELECT Subject, Predicate from Triples t1
        |  left semi join  VP_Creator t2
        |  ON t1.Object=t2.Object WHERE  t1.Object='_:b3091'
        |
        |
        |)
        |AS L (subject,predicate)
      """.stripMargin

}
