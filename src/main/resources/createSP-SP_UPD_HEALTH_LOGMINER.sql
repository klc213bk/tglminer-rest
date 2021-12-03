CREATE OR REPLACE PROCEDURE SP_UPD_HEALTH_LOGMINER (
    i_scn NUMBER,
    i_heartbeat_time TIMESTAMP,
    i_received TIMESTAMP
) AS 
BEGIN
  
    update TM_HEALTH 
    set LOGMINER_SCN=i_scn, LOGMINER_RECEIVED=i_received
    where HEARTBEAT_TIME = i_heartbeat_time;
                
END SP_UPD_HEALTH_LOGMINER;