package com.original.channel.model;

public enum ChannelType {

	// GooglePlay渠道
	GP("GP"),

	// 非GooglePlay渠道
	NONGP("nonGP"),

	// 没有被系统监控的渠道类型
	DEFAULT("default");

	private String name;

	ChannelType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public static ChannelType getType(String channelType) {
		if (null != channelType) {
			try {
				channelType = channelType.toUpperCase();
				return Enum.valueOf(ChannelType.class, channelType.trim());
			} catch (IllegalArgumentException ex) {
				ex.printStackTrace();
			}
		}
		return null;
	}

}
