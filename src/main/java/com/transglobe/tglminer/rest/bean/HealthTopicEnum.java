package com.transglobe.tglminer.rest.bean;

public enum HealthTopicEnum {
	DDL("EBAOPRD1.TGLMINER._GENERIC_DDL"),
	HEARTBEAT("EBAOPRD1.TGLMINER.HE_HEARTBEAT");
			
	private String topic;
	

	HealthTopicEnum(String topic) {
		this.topic = topic;
	}


	public String getTopic() {
		return topic;
	}


	public void setTopic(String topic) {
		this.topic = topic;
	}

	
	
}
