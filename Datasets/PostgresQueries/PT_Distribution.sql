 SELECT create_distributed_table('abstract','publication');

 SELECT create_distributed_table('author','person');

 SELECT create_distributed_table('document','document');

 SELECT create_distributed_table('document_homepage','document');

 SELECT create_distributed_table('document_seealso','document');

 SELECT create_distributed_table('editor','document');

 SELECT create_distributed_table('person','person');

 SELECT create_distributed_table('publication','publication');

 SELECT create_distributed_table('publication_cdrom','document');

 SELECT create_distributed_table('publicationtype','publication');

 SELECT create_distributed_table('reference','document');

 SELECT create_distributed_table('venue','venue');

 SELECT create_distributed_table('venuetype','document');
