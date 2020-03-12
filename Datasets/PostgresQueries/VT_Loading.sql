\COPY   abstractv   (subject , object )FROM '/data/Disk1/500M/CSV/VT/abstract.csv' DELIMITER ',' CSV HEADER;

\COPY   booktitle   (subject , object )FROM '/data/Disk1/500M/CSV/VT/booktitle.csv' DELIMITER ',' CSV HEADER;

\COPY   creator   (subject , object )FROM '/data/Disk1/500M/CSV/VT/creator.csv' DELIMITER ',' CSV HEADER;

\COPY   editorv   (subject , object )FROM '/data/Disk1/500M/CSV/VT/editor.csv' DELIMITER ',' CSV HEADER;

\COPY   homepage   (subject , object )FROM '/data/Disk1/500M/CSV/VT/homepage.csv' DELIMITER ',' CSV HEADER;

\COPY   issued   (subject , object )FROM '/data/Disk1/500M/CSV/VT/issued.csv' DELIMITER ',' CSV HEADER;

\COPY   journal   (subject , object )FROM '/data/Disk1/500M/CSV/VT/injournal.csv' DELIMITER ',' CSV HEADER;

\COPY   name   (subject , object )FROM '/data/Disk1/500M/CSV/VT/name.csv' DELIMITER ',' CSV HEADER;

\COPY   pages   (subject , object )FROM '/data/Disk1/500M/CSV/VT/pages.csv' DELIMITER ',' CSV HEADER;

\COPY   partof   (subject , object )FROM '/data/Disk1/500M/CSV/VT/partof.csv' DELIMITER ',' CSV HEADER;

\COPY   referencesv   (subject , object )FROM '/data/Disk1/500M/CSV/VT/references.csv' DELIMITER ',' CSV HEADER;

\COPY   seealso   (subject , object )FROM '/data/Disk1/500M/CSV/VT/seealso.csv' DELIMITER ',' CSV HEADER;

\COPY   subclassof   (subject , object )FROM '/data/Disk1/500M/CSV/VT/subclassof.csv' DELIMITER ',' CSV HEADER;

\COPY   title   (subject , object )FROM '/data/Disk1/500M/CSV/VT/title.csv' DELIMITER ',' CSV HEADER;

\COPY   type   (subject , object )FROM '/data/Disk1/500M/CSV/VT/type.csv' DELIMITER ',' CSV HEADER;
