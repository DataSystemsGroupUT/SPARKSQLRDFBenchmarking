CREATE TABLE IF NOT EXISTS rdfbench100k.reference
(document STRING, cited STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS rdfbench100k.publication_cdrom
(publication STRING, cdrom STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS rdfbench100k.publicationtype
(publication STRING, type STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS rdfbench100k.person
(person STRING, name STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS rdfbench100k.editor
(document STRING, person STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS  rdfbench100k.publication  
(publication String, pages String, note String, chapter String, venue String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS rdfbench100k.document_seealso
(document STRING, seealso STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS rdfbench100k.document_homepage
(document STRING, homepage STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS rdfbench100k.abstract
(publication STRING,  txt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS rdfbench100k.document
(document STRING, title	STRING, volume STRING, 	isbn	 STRING, month STRING, 	number STRING, 	series STRING, 	issued	STRING, booktitle STRING, publisher STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS rdfbench100k.author
(person STRING,  document STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;



 	



 	
CREATE TABLE IF NOT EXISTS rdfbench100k.VenueType
(document STRING, type STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE  
tblproperties("skip.header.line.count"="1");


 	
CREATE TABLE IF NOT EXISTS rdfbench100k.Venue
(venue STRING, title STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE  
tblproperties("skip.header.line.count"="1");
