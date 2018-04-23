package com.ijunhai.common.serde;

public class WrapMessage {

	private String dataSystem;
	private String ip;
	private String filePath;
	private int lineCount;
	private byte[] messageBody;

	public WrapMessage(String dataSource, String ip, String filePath, int lineCount, byte[] messageBody) {
		this.dataSystem = dataSource;
		this.ip = ip;
		this.filePath = filePath;
		this.lineCount = lineCount;
		this.messageBody = messageBody;
	}

	public String getDataSystem() {
		return dataSystem;
	}

	public void setDataSystem(String dataSystem) {
		this.dataSystem = dataSystem;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public int getLineCount() {
		return lineCount;
	}

	public void setLineCount(int lineCount) {
		this.lineCount = lineCount;
	}

	public byte[] getMessageBody() {
		return messageBody;
	}

	public void setMessageBody(byte[] messageBody) {
		this.messageBody = messageBody;
	}
}


