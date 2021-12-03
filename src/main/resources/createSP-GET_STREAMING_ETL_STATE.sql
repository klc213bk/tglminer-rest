create or replace PROCEDURE GET_STREAMING_ETL_STATE (
		i_etl_name VARCHAR2
		, o_state OUT VARCHAR2
) AS
  v_received_cnt NUMBER := 0;
  v_state VARCHAR2(30);
  v_client_id VARCHAR2(30);
BEGIN
    
    v_client_id := 'partycontact1-1';
  FOR rec IN (SELECT * FROM TM_HEALTH ORDER BY HEARTBEAT_TIME DESC FETCH NEXT 5 ROWS ONLY)
  LOOP
    IF rec.CONSUMER_CLIENT IS NOT NULL THEN
      IF INSTR(rec.CONSUMER_CLIENT, v_client_id) > 0 THEN
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

	o_state := v_state;

END GET_STREAMING_ETL_STATE;