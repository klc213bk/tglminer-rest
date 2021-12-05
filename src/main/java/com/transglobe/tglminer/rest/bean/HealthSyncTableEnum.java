package com.transglobe.tglminer.rest.bean;

public enum HealthSyncTableEnum {

	SYNC_TABLE_TM_HEARTBEAT("TGLMINER.TM_HEARTBEAT");

	private String syncTableName;
	
	HealthSyncTableEnum(String syncTableName) {
		this.syncTableName = syncTableName;
	}

	public String getSyncTableName() {
		return syncTableName;
	}

	public void setSyncTableName(String syncTableName) {
		this.syncTableName = syncTableName;
	}

	
	
}
