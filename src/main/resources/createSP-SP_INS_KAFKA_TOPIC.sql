CREATE OR REPLACE PROCEDURE SP_INS_KAFKA_TOPIC ( 
    i_etl_name VARCHAR2,
    i_topic VARCHAR2
) AS    
BEGIN

  insert into TM_KAFKA_TOPIC (ETL_NAME, TOPIC) values(i_etl_name, i_topic);
  
END SP_INS_KAFKA_TOPIC;