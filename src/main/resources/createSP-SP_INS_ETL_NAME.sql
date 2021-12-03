CREATE OR REPLACE PROCEDURE SP_INS_ETL_NAME (
	i_cat VARCHAR2,
	i_etl_name VARCHAR2,
	i_with_sync NUMBER,
	i_consumer_status VARCHAR2,
	i_note VARCHAR2
) AS
BEGIN
  			
   insert into TM_ETL_NAME (CAT,ETL_NAME,WITH_SYNC,CONSUMER_STATUS,NOTE) 
   values(i_cat,i_etl_name,i_with_sync,i_consumer_status,i_note);
   
END SP_INS_ETL_NAME;