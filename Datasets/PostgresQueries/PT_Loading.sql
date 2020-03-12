\COPY  abstract  (publication , txt ) FROM '/data/Disk1/500M/CSV/PT/Abstract.csv' DELIMITER ',' CSV HEADER;

\COPY  author  (person, document) FROM '/data/Disk1/500M/CSV/PT/Author.csv' DELIMITER ',' CSV HEADER;

\COPY  document (document, title, volume, isbn, month, number, series, issued, booktitle, publisher) FROM '/data/Disk1/500M/CSV/PT/Document.csv' DELIMITER ',' CSV HEADER;

\COPY  document_homepage  (document , homepage) FROM '/data/Disk1/500M/CSV/PT/Document_homepage.csv' DELIMITER ',' CSV HEADER;

\COPY  document_seealso  (document, seealso) FROM '/data/Disk1/500M/CSV/PT/Document_seeAlso.csv' DELIMITER ',' CSV HEADER;

\COPY  editor  (document, person) FROM '/data/Disk1/500M/CSV/PT/Editor.csv' DELIMITER ',' CSV HEADER;

\COPY  person  (person , name ) FROM '/data/Disk1/500M/CSV/PT/Person.csv' DELIMITER ',' CSV HEADER;

\COPY  publication  (publication , pages , note , chapter, venue) FROM '/data/Disk1/500M/CSV/PT/Publication.csv' DELIMITER ',' CSV HEADER;

\COPY  Publication_cdrom  (document , cdrom ) FROM '/data/Disk1/500M/CSV/PT/Publication_cdrom.csv' DELIMITER ',' CSV HEADER;

\COPY  Publicationtype  (publication , type ) FROM '/data/Disk1/500M/CSV/PT/PublicationType.csv' DELIMITER ',' CSV HEADER;

\COPY  reference  (document ,cited ) FROM '/data/Disk1/500M/CSV/PT/Reference.csv' DELIMITER ',' CSV HEADER;

\COPY  venue  (venue ,title ) FROM '/data/Disk1/500M/CSV/PT/Venue.csv' DELIMITER ',' CSV HEADER;

\COPY  venuetype  (document ,type ) FROM '/data/Disk1/500M/CSV/PT/VenueType.csv' DELIMITER ',' CSV HEADER;
