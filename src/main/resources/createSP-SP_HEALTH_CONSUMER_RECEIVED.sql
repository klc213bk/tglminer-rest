CREATE OR REPLACE PROCEDURE SP_HEALTH_CONSUMER_RECEIVED (
	i_etl_name VARCHAR2,
	i_client_id VARCHAR2,
	i_heartbeat_time TIMESTAMP,
	i_consumer_received TIMESTAMP
) AS
BEGIN
  			
  update TM_HEALTH 
   set CONSUMER_RECEIVED=i_consumer_received,
   CONSUMER_CLIENT=(CONSUMER_CLIENT || i_client_id || ',') 
   where HEARTBEAT_TIME = i_heartbeat_time;
   
   update TM_ETL_NAME
   set CONSUMER_STATUS = 'RECEVING'
   where ETL_NAME = i_etl_name;
   
END SP_HEALTH_CONSUMER_RECEIVED;
