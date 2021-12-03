CREATE OR REPLACE PROCEDURE SP_INS_HEALTH_HEARTBEAT (
	i_heartbeat TIMESTAMP
) AS
	v_current_scn NUMBER(19,0);
	v_cpu_util NUMBER;
BEGIN
   
   select current_scn into v_current_scn from gv$database;
   
   select VALUE into v_cpu_util from V$SYSMETRIC_HISTORY
   where METRIC_ID = 2057 order by BEGIN_TIME desc fetch next 1 row only;
					
   insert into TM_HEARTBEAT (HEARTBEAT_TIME) values(i_heartbeat);
   
   insert into TM_HEALTH (HEARTBEAT_TIME,CURRENT_SCN,CPU_UTIL_VALUE) 
   values (i_heartbeat,v_current_scn,v_cpu_util);
   
END SP_INS_HEALTH_HEARTBEAT;