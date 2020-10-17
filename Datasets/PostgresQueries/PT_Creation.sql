CREATE TABLE IF NOT EXISTS  abstract  (publication Text, txt Text) ;

CREATE TABLE IF NOT EXISTS  author  (person Text, document Text) ;


CREATE TABLE IF NOT EXISTS  document  (document Text, title Text, volume Text, isbn Text, month Text,	number Text,series Text,issued Text,	booktitle Text, publisher Text) ;


CREATE TABLE IF NOT EXISTS  document_homepage  (document Text, homepage Text) ;

CREATE TABLE IF NOT EXISTS  document_seealso  (document Text, seealso Text) ;

CREATE TABLE IF NOT EXISTS  editor  (document Text, person Text) ;


CREATE TABLE IF NOT EXISTS  person  (person Text, name Text) ;

CREATE TABLE IF NOT EXISTS  publication  (publication Text, pages Text, note Text,	chapter Text, venue Text) ;


CREATE TABLE IF NOT EXISTS  Publication_cdrom  (document Text, cdrom Text) ;


CREATE TABLE IF NOT EXISTS  Publicationtype  (publication Text, type Text) ;


CREATE TABLE IF NOT EXISTS  reference  (document Text,cited Text);


CREATE TABLE IF NOT EXISTS  venue  (venue Text,title Text);

CREATE TABLE IF NOT EXISTS  venuetype  (document Text,type Text);
