/*
 * Copyright (c) 2022 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.communicator.lg.lcd;

/**
 * FailOverEnum class defined the enum provides list fail over status
 *
 * @author Kevin / Symphony Dev Team<br>
 * Created on 12/1/2022
 * @since 1.0.0
 */
public enum FailOverEnum {

	OFF("Off", "00"),
	AUTO("Auto", "01"),
	MANUAL("Manual", "02");

	private final String name;
	private final String value;

	FailOverEnum(String name, String value) {
		this.name = name;
		this.value = value;
	}

	/**
	 * Retrieves {@link #name}
	 *
	 * @return value of {@link #name}
	 */
	public String getName() {
		return name;
	}

	/**
	 * Retrieves {@link #value}
	 *
	 * @return value of {@link #value}
	 */
	public String getValue() {
		return value;
	}

	public static String getValueByName(String name) {
		for (FailOverEnum failOverEnum : FailOverEnum.values()) {
			if (failOverEnum.getName().equals(name)) {
				return failOverEnum.getValue();
			}
		}
		return "None";
	}
}