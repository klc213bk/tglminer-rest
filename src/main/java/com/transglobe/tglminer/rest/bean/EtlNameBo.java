package com.transglobe.tglminer.rest.bean;

public class EtlNameBo {
	
	public static enum WithSyncEnum {
		SYNC,NO_SYNC;
	}
	public static enum ConsumerStatusEnum {
		REGISTERED, STARTED, RECEVING, STOPPED;
	}
	
	private String cat;
	
	private String etlName;
	
	private WithSyncEnum withSync;
	
	private ConsumerStatusEnum consumerStatus;
	
	private String note;

	public String getCat() {
		return cat;
	}

	public void setCat(String cat) {
		this.cat = cat;
	}

	public String getEtlName() {
		return etlName;
	}

	public void setEtlName(String etlName) {
		this.etlName = etlName;
	}

	public WithSyncEnum getWithSync() {
		return withSync;
	}

	public void setWithSync(WithSyncEnum withSync) {
		this.withSync = withSync;
	}

	public ConsumerStatusEnum getConsumerStatus() {
		return consumerStatus;
	}

	public void setConsumerStatus(ConsumerStatusEnum consumerStatus) {
		this.consumerStatus = consumerStatus;
	}

	public String getNote() {
		return note;
	}

	public void setNote(String note) {
		this.note = note;
	}

	
}
