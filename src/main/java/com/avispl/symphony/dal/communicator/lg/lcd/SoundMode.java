/*
 * Copyright (c) 2022 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.communicator.lg.lcd;

/**
 * SoundMode class provides during the monitoring and controlling process
 *
 * @author Kevin / Symphony Dev Team<br>
 * Created on 1/11/2023
 * @since 1.0.0
 */
public enum SoundMode {

	STANDARD("Standard", "01"),
	MUSIC("Music", "02"),
	CINEMA("Cinema", "03"),
	SPORTS("Sports", "04"),
	GAME("Game", "05"),
	NEWS("News (Clear Voice IV)", "07");

	private final String name;
	private final String value;

	/**
	 * SoundMode instantiation
	 *
	 * @param name {@link #name}
	 * @param value {@link #value}
	 */
	SoundMode(String name, String value) {
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
}