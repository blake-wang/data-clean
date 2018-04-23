package com.ijunhai.common.serde;

public enum CompressType {
	NONE(0), LZ4(1);

	private int idx;
	CompressType(int idx) {
		this.idx = idx;
	}

	public int getIdx() {
		return idx;
	}

	public static CompressType valueOf(int idx) {
		switch (idx) {
			case 0:
				return NONE;
			case 1:
				return LZ4;
			default:
				return NONE;
		}
	}
}
