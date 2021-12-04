CREATE OR REPLACE PROCEDURE SP_INS_LOGMINER_TABLE ( 
    i_etl_name VARCHAR2,
    i_table_name VARCHAR2
) AS    
BEGIN

  insert into TM_LOGMINER_TABLE (ETL_NAME, TABLE_NAME) 
  values(i_etl_name, i_table_name);
  
END SP_INS_LOGMINER_TABLE;