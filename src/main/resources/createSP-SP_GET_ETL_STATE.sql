create or replace PROCEDURE SP_GET_ETL_STATE (
		i_client_id VARCHAR2
		, o_recordset OUT SYS_REFCURSOR
) AS
  v_received_cnt NUMBER := 0;
  v_state VARCHAR2(30);
BEGIN
  FOR rec IN (SELECT * FROM TM_HEALTH ORDER BY HEARTBEAT_TIME DESC FETCH NEXT 2 ROWS ONLY)
  LOOP
    IF rec.CONSUMER_CLIENT IS NOT NULL THEN
      IF INSTR(rec.CONSUMER_CLIENT, i_client_id) > 0 THEN
        v_received_cnt := v_received_cnt + 1;
      END IF;
    END IF;
  END LOOP;
  
  DBMS_OUTPUT.PUT_LINE('v_received_cnt=' || v_received_cnt);
  
  IF v_received_cnt > 0 THEN
    v_state := 'ACTIVE';
  ELSE
    v_state := 'STANDBY';
  END IF;
  
	OPEN o_recordset FOR
		SELECT v_state STATE FROM DUAL;
	
END SP_GET_ETL_STATE;