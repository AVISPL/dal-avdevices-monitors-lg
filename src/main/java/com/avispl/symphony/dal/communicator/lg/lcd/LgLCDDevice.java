/*
 * Copyright (c) 2022 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.lg.lcd;

import java.util.*;

import org.springframework.util.CollectionUtils;

import com.avispl.symphony.api.dal.control.Controller;
import com.avispl.symphony.api.dal.dto.control.ControllableProperty;
import com.avispl.symphony.api.dal.dto.monitor.ExtendedStatistics;
import com.avispl.symphony.api.dal.dto.monitor.Statistics;
import com.avispl.symphony.api.dal.monitor.Monitorable;
import com.avispl.symphony.dal.communicator.SocketCommunicator;
import com.avispl.symphony.dal.communicator.lg.lcd.LgLCDConstants.commandNames;
import com.avispl.symphony.dal.communicator.lg.lcd.LgLCDConstants.controlProperties;
import com.avispl.symphony.dal.communicator.lg.lcd.LgLCDConstants.replyStatusNames;
import com.avispl.symphony.dal.communicator.lg.lcd.LgLCDConstants.statisticsProperties;

/**
 * LG LCD Device Adapter
 *
 * Static Monitored Statistics
 * <li>
 * Input
 * Power
 * Fan
 * SyncStatus
 * </li>
 *
 * Historical Monitored Statistics
 * <li> Temperature </li>
 *
 * @author Harry
 * @version 1.2
 * @since 1.2
 */
public class LgLCDDevice extends SocketCommunicator implements Controller, Monitorable {

	int monitorID;
	private Set<String> historicalProperties = new HashSet<>();

	/**
	 * Constructor set the TCP/IP port to be used as well the default monitor ID
	 */
	public LgLCDDevice() {
		super();

		this.setPort(9761);
		this.monitorID = 1;

		// set list of command success strings (included at the end of response when command succeeds, typically ending with command prompt)
		this.setCommandSuccessList(Collections.singletonList("OK"));
		// set list of error response strings (included at the end of response when command fails, typically ending with command prompt)
		this.setCommandErrorList(Collections.singletonList("NG"));
	}

	/**
	 * Retrieves {@link #historicalProperties}
	 *
	 * @return value of {@link #historicalProperties}
	 */
	public String getHistoricalProperties() {
		return String.join(",", this.historicalProperties);
	}

	/**
	 * Sets {@link #historicalProperties} value
	 *
	 * @param historicalProperties new value of {@link #historicalProperties}
	 */
	public void setHistoricalProperties(String historicalProperties) {
		this.historicalProperties.clear();
		Arrays.asList(historicalProperties.split(",")).forEach(propertyName -> {
			this.historicalProperties.add(propertyName.trim());
		});
	}

	/**
	 * This method is recalled by Symphony to control specific property
	 *
	 * @param controllableProperty This is the property to be controled
	 */
	@Override
	public void controlProperty(ControllableProperty controllableProperty) throws Exception {
		if (controllableProperty.getProperty().equals(controlProperties.power.name())) {
			if (controllableProperty.getValue().toString().equals("1")) {
				powerON();
			} else if (controllableProperty.getValue().toString().equals("0")) {
				powerOFF();
			}
		}
	}

	/**
	 * This method is recalled by Symphony to control a list of properties
	 *
	 * @param controllableProperties This is the list of properties to be controlled
	 * @return byte This returns the calculated xor checksum.
	 */
	@Override
	public void controlProperties(List<ControllableProperty> controllableProperties) {
		if (CollectionUtils.isEmpty(controllableProperties)) {
			throw new IllegalArgumentException("ControllableProperties can not be null or empty");
		}
		for (ControllableProperty p : controllableProperties) {
			try {
				controlProperty(p);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This method is recalled by Symphony to get the list of statistics to be displayed
	 *
	 * @return List<Statistics> This return the list of statistics.
	 */
	@Override
	public List<Statistics> getMultipleStatistics() throws Exception {
		ExtendedStatistics extendedStatistics = new ExtendedStatistics();

		//controllable statistics
		Map<String, String> controllable = new HashMap<String, String>() {{
			put(controlProperties.power.name(), "Toggle");
		}};

		//StaticStatistics
		Map<String, String> statistics = new HashMap<String, String>();

		//dynamicStatistics
		Map<String, String> dynamicStatistics = new HashMap<>();

		//getting power status from device
		String power;

		try {
			power = getPower().name();
			if (power.compareTo("ON") == 0) {
				statistics.put(statisticsProperties.power.name(), "1");
			} else if (power.compareTo("OFF") == 0) {
				statistics.put(statisticsProperties.power.name(), "0");
			}
		} catch (Exception e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("error during getPower", e);
			}
			throw e;
		}

		//getting input status from device
		try {
			statistics.put(statisticsProperties.input.name(), getInput().name());
		} catch (Exception e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("error during getInput", e);
			}
			throw e;
		}

		//getting temperature status from device and put into dynamicStatistic
		try {
			String temperatureParameter = statisticsProperties.temperature.name();
			String temperatureValue = String.valueOf(getTemperature());
			if (!historicalProperties.isEmpty() && historicalProperties.contains(temperatureParameter)) {
				dynamicStatistics.put(temperatureParameter, temperatureValue);
			} else {
				statistics.put(temperatureParameter, temperatureValue);
			}
		} catch (Exception e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("error during getInput", e);
			}
			throw e;
		}

		//getting fan status from device
		try {
			statistics.put(statisticsProperties.fan.name(), getFanStatus().name());
		} catch (Exception e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("error during getFanStatus", e);
			}
			throw e;
		}

		//getting sync status from device
		try {
			statistics.put(statisticsProperties.signal.name(), getSyncStatus().name());
		} catch (Exception e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("error during getSyncStatus", e);
			}
			throw e;
		}

		destroyChannel();

		extendedStatistics.setControl(controllable);
		extendedStatistics.setStatistics(statistics);
		extendedStatistics.setDynamicStatistics(dynamicStatistics);

		return Collections.singletonList(extendedStatistics);
	}

	/**
	 * This method is recalled by Symphony to get the current monitor ID (Future purpose)
	 *
	 * @return int This returns the current monitor ID.
	 */
	public int getMonitorID() {
		return monitorID;
	}

	/**
	 * This method is is used by Symphony to set the monitor ID (FUture purpose)
	 *
	 * @param monitorID This is the monitor ID to be set
	 */
	public void setMonitorID(int monitorID) {
		this.monitorID = monitorID;
	}

	/**
	 * This method is used to get the current display power status
	 *
	 * @return powerStatus This returns the calculated xor checksum.
	 */
	private LgLCDConstants.powerStatusNames getPower() {

		try {
			byte[] response = send(LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(LgLCDConstants.commandNames.POWER), LgLCDConstants.commands.get(commandNames.GET)));

			LgLCDConstants.powerStatusNames power = (LgLCDConstants.powerStatusNames) digestResponse(response, LgLCDConstants.commandNames.POWER);

			if (power == null) {
				return LgLCDConstants.powerStatusNames.OFF;
			} else {
				return power;
			}
		} catch (Exception e) {
			if (this.logger.isErrorEnabled()) {
				this.logger.error("error during get power send", e);
			}
		}
		return null;
	}

	protected void powerON() {
		try {
			byte[] response = send(
					LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(LgLCDConstants.commandNames.POWER), LgLCDConstants.powerStatus.get(LgLCDConstants.powerStatusNames.ON)));

			digestResponse(response, LgLCDConstants.commandNames.POWER);
		} catch (Exception e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("error during power OFF send", e);
			}
		}
	}

	protected void powerOFF() {
		try {
			byte[] response = send(
					LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(LgLCDConstants.commandNames.POWER), LgLCDConstants.powerStatus.get(LgLCDConstants.powerStatusNames.OFF)));

			digestResponse(response, LgLCDConstants.commandNames.POWER);
		} catch (Exception e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("error during power ON send", e);
			}
		}
	}

	/**
	 * This method is used to get the current display input
	 *
	 * @return inputNames This returns the current input.
	 */
	private LgLCDConstants.inputNames getInput() {
		try {
			byte[] response = send(LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(commandNames.INPUT), LgLCDConstants.commands.get(commandNames.GET)));

			LgLCDConstants.inputNames input = (LgLCDConstants.inputNames) digestResponse(response, LgLCDConstants.commandNames.INPUT);

			return input;
		} catch (Exception e) {
			this.logger.error("Connect exception", e);
			return LgLCDConstants.inputNames.OFF;
		}
	}

	/**
	 * This method is used to get the current fan status
	 *
	 * @return fanStatusNames This returns the current display fan status.
	 */
	private LgLCDConstants.fanStatusNames getFanStatus() {
		try {
			byte[] response = send(
					LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(LgLCDConstants.commandNames.FANSTATUS), LgLCDConstants.commands.get(LgLCDConstants.commandNames.GET)));

			LgLCDConstants.fanStatusNames fanStatus = (LgLCDConstants.fanStatusNames) digestResponse(response, LgLCDConstants.commandNames.FANSTATUS);

			return fanStatus;
		} catch (Exception e) {
			this.logger.error("Connect exception", e);
			return LgLCDConstants.fanStatusNames.NO_FAN;
		}
	}

	/**
	 * This method is used to get the current display temperature
	 *
	 * @return int This returns the current display temperature.
	 */
	private Integer getTemperature() {
		try {
			byte[] response = send(
					LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(LgLCDConstants.commandNames.TEMPERATURE), LgLCDConstants.commands.get(LgLCDConstants.commandNames.GET)));

			Integer temperature = (Integer) digestResponse(response, LgLCDConstants.commandNames.TEMPERATURE);

			return temperature;
		} catch (Exception e) {
			this.logger.error("Connect exception", e);
			return 0;
		}
	}

	/**
	 * This method is used to get the current display sync status
	 *
	 * @return syncStatusNames This returns the current display sync status.
	 */
	private LgLCDConstants.syncStatusNames getSyncStatus() {
		try {
			byte[] response = send(LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(LgLCDConstants.commandNames.STATUS), LgLCDConstants.signalStatus));

			LgLCDConstants.syncStatusNames status = (LgLCDConstants.syncStatusNames) digestResponse(response, LgLCDConstants.commandNames.STATUS);

			return status;
		} catch (Exception e) {
			this.logger.error("Connect exception", e);
			return LgLCDConstants.syncStatusNames.NO_SYNC;
		}
	}

	/**
	 * This method is used to digest the response received from the device
	 *
	 * @param response This is the response to be digested
	 * @param expectedResponse This is the expected response type to be compared with received
	 * @return Object This returns the result digested from the response.
	 */
	protected Object digestResponse(byte[] response, commandNames expectedResponse) {
		if (response[0] == LgLCDConstants.commands.get(expectedResponse)[1]) {

			byte[] responseStatus = Arrays.copyOfRange(response, 5, 7);

			if (Arrays.equals(responseStatus, LgLCDConstants.replyStatusCodes.get(replyStatusNames.OK))) {

				byte[] reply = Arrays.copyOfRange(response, 7, 9);

				switch (expectedResponse) {
					case POWER: {
						for (Map.Entry<LgLCDConstants.powerStatusNames, byte[]> entry : LgLCDConstants.powerStatus.entrySet()) {
							if (Arrays.equals(reply, entry.getValue())) {
								LgLCDConstants.powerStatusNames power = entry.getKey();
								return power;
							}
						}
					}
					case INPUT: {
						for (Map.Entry<LgLCDConstants.inputNames, byte[]> entry : LgLCDConstants.inputs.entrySet()) {
							if (Arrays.equals(reply, entry.getValue())) {
								LgLCDConstants.inputNames input = entry.getKey();
								return input;
							}
						}
					}
					case TEMPERATURE: {
						return Integer.parseInt(new String(reply), 16);
					}
					case FANSTATUS: {
						for (Map.Entry<LgLCDConstants.fanStatusNames, byte[]> entry : LgLCDConstants.fanStatusCodes.entrySet()) {
							if (Arrays.equals(reply, entry.getValue())) {
								LgLCDConstants.fanStatusNames fanStatus = entry.getKey();
								return fanStatus;
							}
						}
					}
					case STATUS: {
						reply = Arrays.copyOfRange(response, 7, 11);
						for (Map.Entry<LgLCDConstants.syncStatusNames, byte[]> entry : LgLCDConstants.syncStatusCodes.entrySet()) {
							if (Arrays.equals(reply, entry.getValue())) {
								LgLCDConstants.syncStatusNames syncStatus = entry.getKey();
								return syncStatus;
							}
						}
					}
				}
			} else if (Arrays.equals(responseStatus, LgLCDConstants.replyStatusCodes.get(replyStatusNames.NG))) {
				switch (expectedResponse) {
					case FANSTATUS: {
						return LgLCDConstants.fanStatusNames.NOT_SUPPORTED;
					}
					default: {
						if (this.logger.isErrorEnabled()) {
							this.logger.error("error: NG reply: " + this.host + " port: " + this.getPort());
						}
						throw new RuntimeException("NG reply");
					}
				}
			}
		} else {
			if (this.logger.isErrorEnabled()) {
				this.logger.error("error: Unexpected reply: " + this.host + " port: " + this.getPort());
			}
			throw new RuntimeException("Unexpected reply");
		}

		return null;
	}
}