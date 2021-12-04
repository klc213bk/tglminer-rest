package com.transglobe.tglminer.rest.bean;

public enum TglminerTableEnum {
	TM_ETL_NAME("TGLMINER", "TM_ETL_NAME", "createtable-TM_ETL_NAME.sql"),
	TM_KAFKA_TOPIC("TGLMINER", "TM_KAFKA_TOPIC", "createtable-TM_KAFKA_TOPIC.sql"),
	TM_LOGMINER_TABLE("TGLMINER", "TM_LOGMINER_TABLE", "createtable-TM_LOGMINER_TABLE.sql"),
	TM_LOGMINER_OFFSET("TGLMINER", "TM_LOGMINER_OFFSET","createtable-TM_LOGMINER_OFFSET.sql"),
	TM_HEALTH("TGLMINER", "TM_HEALTH", "createtable-TM_HEALTH.sql"),
	TM_HEARTBEAT("TGLMINER", "TM_HEARTBEAT", "createtable-TM_HEARTBEAT.sql"),
	TM_HEALTH_SINK("TGLMINER", "TM_HEALTH_SINK", "createtable-TM_HEALTH_SINK.sql");
	private String schema;
	
	private String tableName;
	
	private String scriptFile;

	TglminerTableEnum(String schema, String tableName, String scriptFile) {
		this.schema = schema;
		this.tableName = tableName;
		this.scriptFile = scriptFile;
	}

	
	public String getSchema() {
		return schema;
	}


	public void setSchema(String schema) {
		this.schema = schema;
	}


	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


	public String getScriptFile() {
		return scriptFile;
	}

	public void setScriptFile(String scriptFile) {
		this.scriptFile = scriptFile;
	}
	
	
}
