package com.transglobe.tglminer.rest.bean;

public enum TglminerSPEnum {
	SP_INS_ETL_NAME("SP_INS_ETL_NAME", "createSP-SP_INS_ETL_NAME.sql"),
	SP_INS_KAFKA_TOPIC("SP_INS_KAFKA_TOPIC", "createSP-SP_INS_KAFKA_TOPIC.sql"),
	SP_GET_ETL_STATE("SP_GET_ETL_STATE", "createSP-SP_GET_ETL_STATE.sql"),
	GET_STREAMING_ETL_STATE("GET_STREAMING_ETL_STATE", "createSP-GET_STREAMING_ETL_STATE.sql"),
	SP_HEALTH_CONSUMER_RECEIVED("SP_HEALTH_CONSUMER_RECEIVED", "createSP-SP_HEALTH_CONSUMER_RECEIVED.sql"),
	SP_INS_HEALTH_HEARTBEAT("SP_INS_HEALTH_HEARTBEAT", "createSP-SP_INS_HEALTH_HEARTBEAT.sql"),
	SP_UPD_HEALTH_LOGMINER("SP_UPD_HEALTH_LOGMINER", "createSP-SP_UPD_HEALTH_LOGMINER.sql");
	
	
	private String spName;
	private String scriptFile;

	TglminerSPEnum(String spName, String scriptFile) {
		this.spName = spName;
		this.scriptFile = scriptFile;
	}

	public String getSpName() {
		return spName;
	}

	public void setSpName(String spName) {
		this.spName = spName;
	}

	public String getScriptFile() {
		return scriptFile;
	}

	public void setScriptFile(String scriptFile) {
		this.scriptFile = scriptFile;
	}
	
	
}
