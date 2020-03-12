CREATE TABLE IF NOT EXISTS  rdfbench100k.abstractv   (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS  rdfbench100k.booktitle   (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS  rdfbench100k.creator  (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS  rdfbench100k.editorv  (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS  rdfbench100k.homepage  (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS  rdfbench100k.journal   (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS  rdfbench100k.issued   (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS  rdfbench100k.name   (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;



CREATE TABLE IF NOT EXISTS  rdfbench100k.pages   (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS  rdfbench100k.partof   (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;


CREATE TABLE IF NOT EXISTS  rdfbench100k.subclassof   (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS  rdfbench100k.title   (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS  rdfbench100k.type   (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;



CREATE TABLE IF NOT EXISTS  rdfbench100k.seealso  (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS  rdfbench100k.referencesv  (subject STRING, object STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1")
;
