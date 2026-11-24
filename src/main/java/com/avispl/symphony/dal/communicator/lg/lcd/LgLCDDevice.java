/*
 * Copyright (c) 2022 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.lg.lcd;

import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.springframework.util.CollectionUtils;

import com.avispl.symphony.api.dal.control.Controller;
import com.avispl.symphony.api.dal.dto.control.AdvancedControllableProperty;
import com.avispl.symphony.api.dal.dto.control.ControllableProperty;
import com.avispl.symphony.api.dal.dto.monitor.ExtendedStatistics;
import com.avispl.symphony.api.dal.dto.monitor.Statistics;
import com.avispl.symphony.api.dal.error.ResourceNotReachableException;
import com.avispl.symphony.api.dal.monitor.Monitorable;
import com.avispl.symphony.dal.communicator.SocketCommunicator;
import com.avispl.symphony.dal.communicator.lg.lcd.LgLCDConstants.commandNames;
import com.avispl.symphony.dal.communicator.lg.lcd.LgLCDConstants.controlProperties;
import com.avispl.symphony.dal.communicator.lg.lcd.LgLCDConstants.fanStatusNames;
import com.avispl.symphony.dal.communicator.lg.lcd.LgLCDConstants.replyStatusNames;
import com.avispl.symphony.dal.communicator.lg.lcd.LgLCDConstants.syncStatusNames;
import com.avispl.symphony.dal.util.StringUtils;

/**
 * LG LCD Device Adapter
 * An implementation of SocketCommunicator to provide communication and interaction with LG device
 *
 * Static Monitored Statistics
 * <li>
 * Temperature(C), SerialNumber, SoftwareVersion, InputSignal, InputSelect, Fan, StandbyMode, TileMode,
 * FailOverMode, SoftwareVersion, SerialNumber, DateTim, SubNetmask, IPAddress, DNSServer, Gateway
 * </li>
 *
 * Management Control
 *
 * Display
 * <li>
 * Power, Language, BackLight(%), Mute, Volume(%), AspectRatio, BrightnessSize, PictureMode, Contrast, Brightness, Sharpness, Color, Tint, ColorTemperature(K), Balance, SoundMode
 * </li>
 *
 * FailOver
 * <li>
 * FailOverMode, InputPriority, PriorityInput, PriorityUp, PriorityDown
 * </li>
 *
 * Input
 * <li>
 * InputType, InputSelect
 * </li>
 * Historical Monitored Statistics
 * <li> Temperature </li>
 *
 * @author Harry, Kevin
 * @version 1.4.0
 * @since 1.4.0
 */
public class LgLCDDevice extends SocketCommunicator implements Controller, Monitorable {

	/**
	 * Adapter metadata (version, build date, uptime), reported in the AdapterMetadata group
	 */
	private final AdapterMetadata adapterMetadata;

	int monitorID;
	private int currentCommandIndex = 0;
	private int defaultConfigTimeout;
	private int currentCachingLifetime;
	private int pollingIntervalInIntValue;
	private int currentGetMultipleInPollingInterval = 0;
	private boolean isEmergencyDelivery;
	private final Set<String> historicalProperties = new HashSet<>();
	private final Set<String> failedMonitor = new HashSet<>();
	private int localCachedFailedMonitor = 0;
	private Map<String, String> cacheMapOfPriorityInputAndValue = new HashMap<>();
	private int countControlUnavailable = 0;
	private ExtendedStatistics localExtendedStatistics;

	/**
	 * a variable to check the adapter init
	 */
	private boolean isFirstInit;

	/**
	 * To avoid timeout errors, caused by the unavailability of the control protocol, all polling-dependent communication operations (monitoring)
	 * should be performed asynchronously. This executor service executes such operations.
	 */
	private ExecutorService fetchingDataExSer;
	private ExecutorService timeoutManagementExSer;

	/**
	 * Local caching to store failed requests after a period of time
	 */
	private final Map<String, Integer> localCachingLifeTimeOfMap = new HashMap<>();

	/**
	 * Local cache stores data after a period of time
	 */
	private final Map<String, String> localCacheMapOfPropertyNameAndValue = new HashMap<>();

	/**
	 * store pollingInterval adapter properties
	 */
	private String pollingInterval;

	/**
	 * store configTimeout adapter properties
	 */
	private String configTimeout;

	/**
	 * Timestamp of the latest command sent to a device.
	 */
	private long lastCommandTimestamp;

	/**
	 * Apply default delay in between of all the commands performed by the adapter.
	 */
	private long commandsCoolDownDelay;

	/**
	 * store cachingLifetime adapter properties
	 */
	private String cachingLifetime;

	/**
	 * store delayTimeInterVal adapter properties
	 */
	private String coolDownDelay;

	/**
	 * store configManagement adapter properties
	 */
	private String configManagement;

	/**
	 * configManagement in boolean value
	 */
	private boolean isConfigManagement;

	/**
	 * ReentrantLock to prevent null pointer exception to localExtendedStatistics when controlProperty method is called before GetMultipleStatistics method.
	 */
	private final ReentrantLock reentrantLock = new ReentrantLock();

	/**
	 * Using Condition to pause the current thread execution until control is complete
	 */
	private final Condition condition = reentrantLock.newCondition();

	/**
	 * {@inheritDoc}
	 *
	 * Override the send() method to add a cool down delay time after every send command
	 */
	@Override
	public byte[] send(byte[] data) throws Exception {
		try {
			long currentTime = System.currentTimeMillis() - lastCommandTimestamp;
			//check next command wait commandsCoolDownDelay time
			if (currentTime < commandsCoolDownDelay) {
				Thread.sleep(commandsCoolDownDelay - currentTime);
			}
			lastCommandTimestamp = System.currentTimeMillis();
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Issuing command %s, timestamp: %s", data, lastCommandTimestamp));
			}
			return super.send(data);
		} finally {
			logger.debug("send data command successfully");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void internalInit() throws Exception {
		fetchingDataExSer = Executors.newFixedThreadPool(1);
		timeoutManagementExSer = Executors.newFixedThreadPool(1);
		isFirstInit = false;
		super.internalInit();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void internalDestroy() {
		if (localExtendedStatistics != null && localExtendedStatistics.getStatistics() != null && localExtendedStatistics.getControllableProperties() != null) {
			localExtendedStatistics.getStatistics().clear();
			localExtendedStatistics.getControllableProperties().clear();
		}
		if (!cacheMapOfPriorityInputAndValue.isEmpty()) {
			cacheMapOfPriorityInputAndValue.clear();
		}

		if (!localCacheMapOfPropertyNameAndValue.isEmpty()) {
			localCacheMapOfPropertyNameAndValue.clear();
		}
		isConfigManagement = false;
		failedMonitor.clear();
		localCachingLifeTimeOfMap.clear();
		try {
			fetchingDataExSer.shutdownNow();
			timeoutManagementExSer.shutdownNow();
		} catch (Exception e) {
			logger.warn("Unable to end the TCP connection.", e);
		} finally {
			super.internalDestroy();
		}
	}

	/**
	 * Constructor set the TCP/IP port to be used as well the default monitor ID
	 */
	public LgLCDDevice() {
		super();
		adapterMetadata = new AdapterMetadata();
		this.setPort(9761);
		this.monitorID = 1;

		// set list of command success strings (included at the end of response when command succeeds, typically ending with command prompt)
		this.setCommandSuccessList(Collections.singletonList("OK"));
		// set list of error response strings (included at the end of response when command fails, typically ending with command prompt)
		this.setCommandErrorList(Collections.singletonList("NG"));
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 *
	 * Check for available devices before retrieving the value
	 * ping latency information to Symphony
	 */
	@Override
	public int ping() throws Exception {
		if (isInitialized()) {
			long pingResultTotal = 0L;

			for (int i = 0; i < this.getPingAttempts(); i++) {
				long startTime = System.currentTimeMillis();

				try (Socket puSocketConnection = new Socket(this.host, this.getPort())) {
					puSocketConnection.setSoTimeout(this.getPingTimeout());
					if (puSocketConnection.isConnected()) {
						long pingResult = System.currentTimeMillis() - startTime;
						pingResultTotal += pingResult;
						if (this.logger.isTraceEnabled()) {
							this.logger.trace(String.format("PING OK: Attempt #%s to connect to %s on port %s succeeded in %s ms", i + 1, host, this.getPort(), pingResult));
						}
					} else {
						if (this.logger.isDebugEnabled()) {
							this.logger.debug(String.format("PING DISCONNECTED: Connection to %s did not succeed within the timeout period of %sms", host, this.getPingTimeout()));
						}
						return this.getPingTimeout();
					}
				} catch (SocketTimeoutException | ConnectException tex) {
					if (this.logger.isDebugEnabled()) {
						this.logger.error(String.format("PING TIMEOUT: Connection to %s did not succeed within the timeout period of %sms", host, this.getPingTimeout()));
					}
					throw new SocketTimeoutException("Connection timed out");
				} catch (Exception e) {
					if (this.logger.isDebugEnabled()) {
						this.logger.error(String.format("PING TIMEOUT: Connection to %s did not succeed, UNKNOWN ERROR %s: ", host, e.getMessage()));
					}
					return this.getPingTimeout();
				}
			}
			return Math.max(1, Math.toIntExact(pingResultTotal / this.getPingAttempts()));
		} else {
			throw new IllegalStateException("Cannot use device class without calling init() first");
		}
	}

	/**
	 * Retrieves {@link #configTimeout}
	 *
	 * @return value of {@link #configTimeout}
	 */
	public String getConfigTimeout() {
		return configTimeout;
	}

	/**
	 * Sets {@link #configTimeout} value
	 *
	 * @param configTimeout new value of {@link #configTimeout}
	 */
	public void setConfigTimeout(String configTimeout) {
		this.configTimeout = configTimeout;
	}

	/**
	 * Retrieves {@link #cachingLifetime}
	 *
	 * @return value of {@link #cachingLifetime}
	 */
	public String getCachingLifetime() {
		return cachingLifetime;
	}

	/**
	 * Sets {@link #cachingLifetime} value
	 *
	 * @param cachingLifetime new value of {@link #cachingLifetime}
	 */
	public void setCachingLifetime(String cachingLifetime) {
		this.cachingLifetime = cachingLifetime;
	}

	/**
	 * Retrieves {@link #configManagement}
	 *
	 * @return value of {@link #configManagement}
	 */
	public String getConfigManagement() {
		return configManagement;
	}

	/**
	 * Sets {@link #configManagement} value
	 *
	 * @param configManagement new value of {@link #configManagement}
	 */
	public void setConfigManagement(String configManagement) {
		this.configManagement = configManagement;
	}

	/**
	 * Pool for keeping all the async operations in, to track any operations in progress and cancel them if needed
	 */
	private final List<Future> devicesExecutionPool = new CopyOnWriteArrayList<>();

	/**
	 * Retrieves {@link #historicalProperties}
	 *
	 * @return value of {@link #historicalProperties}
	 */
	public String getHistoricalProperties() {
		return String.join(LgLCDConstants.COMMA, this.historicalProperties);
	}

	/**
	 * Sets {@link #historicalProperties} value
	 *
	 * @param historicalProperties new value of {@link #historicalProperties}
	 */
	public void setHistoricalProperties(String historicalProperties) {
		this.historicalProperties.clear();
		Arrays.asList(historicalProperties.split(LgLCDConstants.COMMA)).forEach(propertyName -> this.historicalProperties.add(propertyName.trim()));
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
	 * This method is used by Symphony to set the monitor ID (Future purpose)
	 *
	 * @param monitorID This is the monitor ID to be set
	 */
	public void setMonitorID(int monitorID) {
		this.monitorID = monitorID;
	}

	/**
	 * Retrieves {@link #coolDownDelay}
	 *
	 * @return value of {@link #coolDownDelay}
	 */
	public String getCoolDownDelay() {
		return coolDownDelay;
	}

	/**
	 * Sets {@link #coolDownDelay} value
	 *
	 * @param coolDownDelay new value of {@link #coolDownDelay}
	 */
	public void setCoolDownDelay(String coolDownDelay) {
		this.coolDownDelay = coolDownDelay;
	}

	/**
	 * Retrieves {@link #pollingInterval}
	 *
	 * @return value of {@link #pollingInterval}
	 */
	public String getPollingInterval() {
		return pollingInterval;
	}

	/**
	 * Sets {@link #pollingInterval} value
	 *
	 * @param pollingInterval new value of {@link #pollingInterval}
	 */
	public void setPollingInterval(String pollingInterval) {
		this.pollingInterval = pollingInterval;
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
	 * This method is recalled by Symphony to control specific property
	 *
	 * @param controllableProperty This is the property to be controlled
	 */
	@Override
	public void controlProperty(ControllableProperty controllableProperty) throws Exception {
		reentrantLock.lock();
		try {
			if (localExtendedStatistics == null) {
				return;
			}
			condition.signal();
			isEmergencyDelivery = true;
			Map<String, String> stats = this.localExtendedStatistics.getStatistics();
			List<AdvancedControllableProperty> advancedControllableProperties = this.localExtendedStatistics.getControllableProperties();
			String value = String.valueOf(controllableProperty.getValue());
			String property = controllableProperty.getProperty();
			if (controllableProperty.getProperty().equalsIgnoreCase(controlProperties.power.name())) {
				if (controllableProperty.getValue().toString().equals(String.valueOf(LgLCDConstants.NUMBER_ONE))) {
					powerON();
				} else if (controllableProperty.getValue().toString().equals(String.valueOf(LgLCDConstants.ZERO))) {
					powerOFF();
				}
			} else {
				String propertyKey;
				String[] propertyList = property.split(LgLCDConstants.HASH);
				String group = property + LgLCDConstants.HASH;
				if (property.contains(LgLCDConstants.HASH)) {
					propertyKey = propertyList[1];
					group = propertyList[0] + LgLCDConstants.HASH;
				} else {
					propertyKey = property;
				}
				LgControllingCommand lgControllingCommand = LgControllingCommand.getCommandByName(propertyKey);
				switch (lgControllingCommand) {
					case VOLUME:
						String dataConvert = Integer.toHexString((int) Float.parseFloat(value));
						sendRequestToControlValue(commandNames.VOLUME, dataConvert.getBytes(StandardCharsets.UTF_8), false, value);
						String volumeValue = String.valueOf((int) Float.parseFloat(value));
						stats.put(group + LgLCDConstants.VOLUME_VALUE, volumeValue);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.VOLUME, volumeValue);
						updateValueForTheControllableProperty(group + LgLCDConstants.MUTE, String.valueOf(LgLCDConstants.ZERO), stats, advancedControllableProperties);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.MUTE, String.valueOf(LgLCDConstants.ZERO));
						break;
					case MUTE:
						String mute = LgLCDConstants.UNMUTE_VALUE;
						if (String.valueOf(LgLCDConstants.NUMBER_ONE).equals(value)) {
							mute = LgLCDConstants.MUTE_VALUE;
						}
						sendRequestToControlValue(commandNames.MUTE, mute.getBytes(StandardCharsets.UTF_8), false, value);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.MUTE, String.valueOf(Integer.parseInt(mute)));
						break;
					case BACKLIGHT:
						dataConvert = Integer.toHexString((int) Float.parseFloat(value));
						sendRequestToControlValue(commandNames.BACKLIGHT, dataConvert.getBytes(StandardCharsets.UTF_8), false, value);
						String backlight = String.valueOf((int) Float.parseFloat(value));
						stats.put(group + LgLCDConstants.BACKLIGHT_VALUE, backlight);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.BACKLIGHT, backlight);
						break;
					case INPUT_SELECT:
						dataConvert = InputSourceDropdown.getValueOfEnumByNameAndType(value, true);
						try {
							sendRequestToControlValue(commandNames.INPUT_SELECT, dataConvert.getBytes(StandardCharsets.UTF_8), true, value);
						} catch (Exception e) {
							dataConvert = InputSourceDropdown.getValueOfEnumByNameAndType(value, false);
							sendRequestToControlValue(commandNames.INPUT_SELECT, dataConvert.getBytes(StandardCharsets.UTF_8), true, value);
						}
						String inputSelect = getValueByName(LgLCDConstants.INPUT_SELECT);
						stats.put(LgLCDConstants.INPUT_SELECT, inputSelect);
						retrieveDataByCommandName(commandNames.SYNC_STATUS, commandNames.SYNC_STATUS_PARAM, lgControllingCommand);
						String signal = getValueByName(LgLCDConstants.SIGNAL);
						stats.put(LgLCDConstants.SIGNAL, signal);
						stats.put(group + LgLCDConstants.SIGNAL, signal);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.SIGNAL, signal);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.INPUT_SELECT, inputSelect);
						break;
					case POWER_MANAGEMENT_MODE:
						dataConvert = LgLCDConstants.BYTE_COMMAND + EnumTypeHandler.getValueOfEnumByName(PowerManagementModeEnum.class, value);
						sendRequestToControlValue(commandNames.POWER_MANAGEMENT_MODE, dataConvert.getBytes(StandardCharsets.UTF_8), true, value);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.POWER_MANAGEMENT_MODE, value);
						break;
					case DISPLAY_STAND_BY_MODE:
						dataConvert = EnumTypeHandler.getValueOfEnumByName(PowerManagement.class, value);
						sendRequestToControlValue(commandNames.DISPLAY_STAND_BY_MODE, dataConvert.getBytes(StandardCharsets.UTF_8), true, value);
						if (LgLCDConstants.OFF.equalsIgnoreCase(value)) {
							stats.put(LgLCDConstants.DISPLAY_STAND_BY_MODE, LgLCDConstants.OFF);
						} else {
							stats.put(LgLCDConstants.DISPLAY_STAND_BY_MODE, LgLCDConstants.ON);
						}
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.DISPLAY_STAND_BY_MODE, value);
						break;
					case FAILOVER:
						String inputPriority = group + LgLCDConstants.INPUT_PRIORITY;
						String priorityInput = group + LgLCDConstants.PRIORITY_INPUT;
						String priorityInputDown = group + LgLCDConstants.PRIORITY_DOWN;
						String priorityInputUp = group + LgLCDConstants.PRIORITY_UP;
						int failOverStatus = Integer.parseInt(value);
						String failOverName = LgLCDConstants.OFF;
						if (failOverStatus == LgLCDConstants.ZERO) {
							sendRequestToControlValue(commandNames.FAILOVER, FailOverEnum.OFF.getValue().getBytes(StandardCharsets.UTF_8), false, value);
							//Remove all priority 0,1,2,3.etc, priorityInput, and inputPriority.
							stats.remove(inputPriority);
							advancedControllableProperties.removeIf(item -> item.getName().equals(inputPriority));

							stats.remove(priorityInput);
							advancedControllableProperties.removeIf(item -> item.getName().equals(priorityInput));

							stats.remove(priorityInputDown);
							advancedControllableProperties.removeIf(item -> item.getName().equals(priorityInputDown));

							stats.remove(priorityInputUp);
							advancedControllableProperties.removeIf(item -> item.getName().equals(priorityInputUp));

							if (cacheMapOfPriorityInputAndValue != null) {
								for (Entry<String, String> input : cacheMapOfPriorityInputAndValue.entrySet()) {
									stats.remove(group + input.getKey());
								}
							}
						} else if (failOverStatus == LgLCDConstants.NUMBER_ONE) {
							sendRequestToControlValue(commandNames.FAILOVER, FailOverEnum.AUTO.getValue().getBytes(StandardCharsets.UTF_8), false, value);
							updateValueForTheControllableProperty(property, value, stats, advancedControllableProperties);

							AdvancedControllableProperty controlInputPriority = controlSwitch(stats, group + LgLCDConstants.INPUT_PRIORITY, String.valueOf(LgLCDConstants.ZERO),
									LgLCDConstants.AUTO,
									LgLCDConstants.MANUAL);
							checkControlPropertyBeforeAddNewProperty(controlInputPriority, advancedControllableProperties);
							failOverName = LgLCDConstants.AUTO;
						}
						stats.put(LgLCDConstants.FAILOVER_MODE, failOverName);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.FAILOVER_MODE, failOverName);
						break;
					case INPUT_PRIORITY:
						String failoverStatus = LgLCDConstants.AUTO;
						if (String.valueOf(LgLCDConstants.ZERO).equals(value)) {
							if (cacheMapOfPriorityInputAndValue != null) {
								for (Entry<String, String> input : cacheMapOfPriorityInputAndValue.entrySet()) {
									stats.remove(group + input.getKey());
								}
							}
							priorityInputDown = group + LgLCDConstants.PRIORITY_DOWN;
							priorityInputUp = group + LgLCDConstants.PRIORITY_UP;
							priorityInput = group + LgLCDConstants.PRIORITY_INPUT;

							stats.remove(priorityInputDown);
							advancedControllableProperties.removeIf(item -> item.getName().equals(priorityInputDown));

							stats.remove(priorityInputUp);
							advancedControllableProperties.removeIf(item -> item.getName().equals(priorityInputUp));

							stats.remove(priorityInput);
							advancedControllableProperties.removeIf(item -> item.getName().equals(priorityInput));

							sendRequestToControlValue(commandNames.FAILOVER, FailOverEnum.AUTO.getValue().getBytes(StandardCharsets.UTF_8), false, value);
						} else {
							failoverStatus = LgLCDConstants.MANUAL;
							sendRequestToControlValue(commandNames.FAILOVER, FailOverEnum.MANUAL.getValue().getBytes(StandardCharsets.UTF_8), false, value);
							retrieveDataByCommandName(commandNames.FAILOVER_INPUT_LIST, commandNames.GET, lgControllingCommand);
							// failover is Manual
							AdvancedControllableProperty controlInputPriority = controlSwitch(stats, group + LgLCDConstants.INPUT_PRIORITY, String.valueOf(LgLCDConstants.NUMBER_ONE), LgLCDConstants.AUTO,
									LgLCDConstants.MANUAL);
							checkControlPropertyBeforeAddNewProperty(controlInputPriority, advancedControllableProperties);
							for (Entry<String, String> entry : cacheMapOfPriorityInputAndValue.entrySet()) {
								if (LgLCDConstants.PLAY_VIA_URL.equalsIgnoreCase(entry.getValue())) {
									continue;
								}
								stats.put(group + entry.getKey(), entry.getValue());
							}
							stats.put(group + LgLCDConstants.PRIORITY_UP, LgLCDConstants.NA);
							advancedControllableProperties.add(createButton(group + LgLCDConstants.PRIORITY_UP, LgLCDConstants.UP, LgLCDConstants.PROCESSING, 0));

							stats.put(group + LgLCDConstants.PRIORITY_DOWN, LgLCDConstants.NA);
							advancedControllableProperties.add(createButton(group + LgLCDConstants.PRIORITY_DOWN, LgLCDConstants.DOWN, LgLCDConstants.PROCESSING, 0));

							String[] inputSelected = cacheMapOfPriorityInputAndValue.values().stream().filter(item -> !item.equalsIgnoreCase(LgLCDConstants.PLAY_VIA_URL)).collect(Collectors.toList())
									.toArray(new String[0]);

							String inputSourceDefaultValue = getValueByName(LgLCDConstants.PRIORITY_INPUT);
							if (!LgLCDConstants.NA.equals(inputSourceDefaultValue)) {
								Optional<Entry<String, String>> priorityInputOption = cacheMapOfPriorityInputAndValue.entrySet().stream().filter(item -> !item.getValue().equalsIgnoreCase(LgLCDConstants.PLAY_VIA_URL))
										.findFirst();
								if (priorityInputOption.isPresent()) {
									inputSourceDefaultValue = priorityInputOption.get().getValue();
								}
								localCacheMapOfPropertyNameAndValue.put(LgLCDConstants.PRIORITY_INPUT, inputSourceDefaultValue);
							}
							populatePriorityInput(stats, advancedControllableProperties, group, inputSourceDefaultValue);
							AdvancedControllableProperty controlInputSource = controlDropdown(stats, inputSelected, group + LgLCDConstants.PRIORITY_INPUT, inputSourceDefaultValue);
							checkControlPropertyBeforeAddNewProperty(controlInputSource, advancedControllableProperties);
						}
						stats.put(LgLCDConstants.FAILOVER_MODE, failoverStatus);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.FAILOVER_MODE, failoverStatus);
						break;
					case PRIORITY_INPUT:
						localCacheMapOfPropertyNameAndValue.remove(LgLCDConstants.PRIORITY_INPUT);
						localCacheMapOfPropertyNameAndValue.put(propertyKey, value);
						populatePriorityInput(stats, advancedControllableProperties, group, value);
						break;
					case PRIORITY_DOWN:
						String currentPriority = getValueByName(LgLCDConstants.PRIORITY_INPUT);
						Map<String, String> newPriorityMap = new HashMap<>();
						Entry<String, String> priorityKey = cacheMapOfPriorityInputAndValue.entrySet().stream().filter(item -> item.getValue().equals(currentPriority)).findFirst().orElse(null);
						int len = cacheMapOfPriorityInputAndValue.size();
						for (int i = 1; i <= len; i++) {
							String currentKeyOfPriority = LgLCDConstants.PRIORITY + i;
							String previousKeyOfPriority = LgLCDConstants.PRIORITY + (i - 1);
							String nextKeyOfPriority = LgLCDConstants.PRIORITY + (i + 1);
							if (currentPriority.equals(cacheMapOfPriorityInputAndValue.get(LgLCDConstants.PRIORITY + len))) {
								break;
							} else {
								if (priorityKey.getKey().equals(currentKeyOfPriority)) {
									newPriorityMap.put(currentKeyOfPriority, cacheMapOfPriorityInputAndValue.get(nextKeyOfPriority));
								} else if (priorityKey.getKey().equals(previousKeyOfPriority)) {
									newPriorityMap.put(currentKeyOfPriority, cacheMapOfPriorityInputAndValue.get(previousKeyOfPriority));
								} else {
									newPriorityMap.put(currentKeyOfPriority, cacheMapOfPriorityInputAndValue.get(currentKeyOfPriority));
								}
							}
						}
						if (!newPriorityMap.isEmpty()) {
							if (StringUtils.isNullOrEmpty(newPriorityMap.get(LgLCDConstants.PRIORITY + newPriorityMap.size()))) {
								newPriorityMap.remove(LgLCDConstants.PRIORITY + newPriorityMap.size());
							}
							cacheMapOfPriorityInputAndValue = newPriorityMap;
						}
						StringBuilder stringBuilder = new StringBuilder();
						for (String values : cacheMapOfPriorityInputAndValue.values()) {
							if (StringUtils.isNullOrEmpty(values) || LgLCDConstants.PLAY_VIA_URL.equalsIgnoreCase(values)) {
								continue;
							}
							stringBuilder.append(EnumTypeHandler.getValueOfEnumByName(FailOverInputSourceEnum.class, values));
							stringBuilder.append(LgLCDConstants.SPACE);
						}
						sendRequestToControlValue(commandNames.FAILOVER_INPUT_LIST, stringBuilder.substring(0, stringBuilder.length() - 1).getBytes(StandardCharsets.UTF_8), false, value);
						for (Entry<String, String> input : cacheMapOfPriorityInputAndValue.entrySet()) {
							stats.put(group + input.getKey(), input.getValue());
						}
						populatePriorityInput(stats, advancedControllableProperties, group, currentPriority);
						break;
					case PRIORITY_UP:
						currentPriority = getValueByName(LgLCDConstants.PRIORITY_INPUT);
						newPriorityMap = new HashMap<>();
						priorityKey = cacheMapOfPriorityInputAndValue.entrySet().stream().filter(item -> item.getValue().equals(currentPriority)).findFirst().orElse(null);
						len = cacheMapOfPriorityInputAndValue.size();
						for (int i = 1; i <= len; i++) {
							String currentKeyOfPriority = LgLCDConstants.PRIORITY + i;
							String previousKeyOfPriority = LgLCDConstants.PRIORITY + (i - 1);
							String nextKeyOfPriority = LgLCDConstants.PRIORITY + (i + 1);
							if (currentPriority.equals(cacheMapOfPriorityInputAndValue.get(LgLCDConstants.PRIORITY + 1))) {
								break;
							} else {
								if (priorityKey.getKey().equals(nextKeyOfPriority)) {
									newPriorityMap.put(currentKeyOfPriority, cacheMapOfPriorityInputAndValue.get(nextKeyOfPriority));
								} else if (priorityKey.getKey().equals(currentKeyOfPriority)) {
									newPriorityMap.put(currentKeyOfPriority, cacheMapOfPriorityInputAndValue.get(previousKeyOfPriority));
								} else {
									newPriorityMap.put(currentKeyOfPriority, cacheMapOfPriorityInputAndValue.get(currentKeyOfPriority));
								}
							}
						}
						if (!newPriorityMap.isEmpty()) {
							if (StringUtils.isNullOrEmpty(newPriorityMap.get(LgLCDConstants.PRIORITY + newPriorityMap.size()))) {
								newPriorityMap.remove(LgLCDConstants.PRIORITY + newPriorityMap.size());
							}
							cacheMapOfPriorityInputAndValue = newPriorityMap;
						}
						stringBuilder = new StringBuilder();
						for (String values : cacheMapOfPriorityInputAndValue.values()) {
							if (StringUtils.isNullOrEmpty(values) || LgLCDConstants.PLAY_VIA_URL.equalsIgnoreCase(values)) {
								continue;
							}
							stringBuilder.append(EnumTypeHandler.getValueOfEnumByName(FailOverInputSourceEnum.class, values));
							stringBuilder.append(LgLCDConstants.SPACE);
						}
						sendRequestToControlValue(commandNames.FAILOVER_INPUT_LIST, stringBuilder.substring(0, stringBuilder.length() - 1).getBytes(StandardCharsets.UTF_8), false, value);
						for (Entry<String, String> entry : cacheMapOfPriorityInputAndValue.entrySet()) {
							if (LgLCDConstants.PLAY_VIA_URL.equalsIgnoreCase(entry.getValue())) {
								continue;
							}
							stats.remove(group + entry.getKey());
							stats.put(group + entry.getKey(), entry.getValue());
						}
						populatePriorityInput(stats, advancedControllableProperties, group, currentPriority);
						break;
					case TILE_MODE:
						String tileModeValue = LgLCDConstants.OFF;
						String naturalModeKey = group + LgLCDConstants.NATURAL_MODE;
						String naturalSize = group + LgLCDConstants.NATURAL_SIZE;
						String tileID = group + LgLCDConstants.TILE_MODE_ID;
						String paramTileMode;
						if (String.valueOf(LgLCDConstants.ZERO).equals(value)) {
							stats.remove(naturalModeKey);
							stats.remove(naturalSize);
							stats.remove(tileID);
							advancedControllableProperties.removeIf(item -> item.getName().equals(naturalModeKey));
							paramTileMode = String.valueOf(LgLCDConstants.ZERO) + LgLCDConstants.ZERO;
							sendRequestToControlValue(commandNames.TILE_MODE_CONTROL, paramTileMode.getBytes(StandardCharsets.UTF_8), false, value);
						} else {
							tileModeValue = LgLCDConstants.ON;
							retrieveDataByCommandName(commandNames.TILE_MODE_SETTINGS, commandNames.GET, lgControllingCommand);
							paramTileMode =
									Integer.toHexString(Integer.parseInt(stats.get(group + LgLCDConstants.TILE_MODE_COLUMN))) + Integer.toHexString(Integer.parseInt(stats.get(group + LgLCDConstants.TILE_MODE_ROW)));
							sendRequestToControlValue(commandNames.TILE_MODE_CONTROL, paramTileMode.getBytes(StandardCharsets.UTF_8), false, value);
							retrieveDataByCommandName(commandNames.NATURAL_MODE, commandNames.GET, lgControllingCommand);
							String naturalMode = getValueByName(LgLCDConstants.NATURAL_MODE);
							if (!LgLCDConstants.NA.equals(naturalMode)) {
								naturalMode = String.valueOf(LgLCDConstants.ZERO == Integer.parseInt(naturalMode) ? 0 : 1);
							}
							AdvancedControllableProperty controlNaturalMode = controlSwitch(stats, group + LgLCDConstants.NATURAL_MODE, naturalMode, LgLCDConstants.OFF, LgLCDConstants.ON);
							checkControlPropertyBeforeAddNewProperty(controlNaturalMode, advancedControllableProperties);
							if (String.valueOf(LgLCDConstants.NUMBER_ONE).equals(naturalMode)) {
								retrieveDataByCommandName(commandNames.NATURAL_SIZE, commandNames.NATURAL_SIZE_PARAM, lgControllingCommand);
								stats.put(group + LgLCDConstants.NATURAL_SIZE, getValueByName(LgLCDConstants.NATURAL_SIZE));
							}
							retrieveDataByCommandName(commandNames.TILE_ID, commandNames.GET, lgControllingCommand);
							String tileModeID = getValueByName(LgLCDConstants.TILE_MODE_ID);
							if (!LgLCDConstants.NA.equals(tileModeID)) {
								tileModeID = String.valueOf(Integer.parseInt(tileModeID));
							}
							stats.put(group + LgLCDConstants.TILE_MODE_ID, tileModeID);
						}
						stats.put(LgLCDConstants.TILE_MODE, tileModeValue);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TILE_MODE, tileModeValue);
						break;
					case NATURAL_MODE:
						naturalSize = group + LgLCDConstants.NATURAL_SIZE;
						String paramNatural = String.valueOf(LgLCDConstants.ZERO);
						if (String.valueOf(LgLCDConstants.ZERO).equals(value)) {
							stats.remove(naturalSize);
							paramNatural = paramNatural + LgLCDConstants.ZERO;
							sendRequestToControlValue(commandNames.NATURAL_MODE, paramNatural.getBytes(StandardCharsets.UTF_8), false, value);
						} else {
							paramNatural = paramNatural + LgLCDConstants.NUMBER_ONE;
							sendRequestToControlValue(commandNames.NATURAL_MODE, paramNatural.getBytes(StandardCharsets.UTF_8), false, value);
							retrieveDataByCommandName(commandNames.NATURAL_SIZE, commandNames.NATURAL_SIZE_PARAM, lgControllingCommand);
							stats.put(group + LgLCDConstants.NATURAL_SIZE, getValueByName(LgLCDConstants.NATURAL_SIZE));
						}
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NATURAL_MODE, String.valueOf(Integer.parseInt(paramNatural)));
						break;
					case BALANCE:
						String balance = EnumTypeHandler.getValueOfEnumByName(Balance.class, value);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), balance.getBytes(StandardCharsets.UTF_8), true, value);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.BALANCE, balance);
						break;
					case BRIGHTNESS:
						int brightness = (int) Float.parseFloat(value);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), Integer.toHexString(brightness).getBytes(StandardCharsets.UTF_8), false, value);
						stats.put(group + LgLCDConstants.BRIGHTNESS_VALUE, String.valueOf(brightness));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.BRIGHTNESS_VALUE, String.valueOf(brightness));
						break;
					case COLOR_TEMPERATURE:
						int colorTemperature = (int) convertFromUIValueToApiValue(String.valueOf((int) Float.parseFloat(value)), LgLCDConstants.COLOR_TEMPERATURE_UI_MAX_VALUE,
								LgLCDConstants.COLOR_TEMPERATURE_UI_MIN_VALUE);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), Integer.toHexString(colorTemperature).getBytes(StandardCharsets.UTF_8), false, value);
						int newValue = (int) convertFromApiValueToUIValue(String.valueOf(colorTemperature), LgLCDConstants.COLOR_TEMPERATURE_MAX_VALUE, LgLCDConstants.COLOR_TEMPERATURE_MIN_VALUE);
						stats.put(group + LgLCDConstants.COLOR_TEMPERATURE_VALUE, String.valueOf(newValue));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.COLOR_TEMPERATURE, String.valueOf(colorTemperature));
						break;
					case CONTRAST:
						int contrast = (int) Float.parseFloat(value);
						dataConvert = Integer.toHexString(contrast);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), dataConvert.getBytes(StandardCharsets.UTF_8), false, value);
						stats.put(group + LgLCDConstants.CONTRAST_VALUE, String.valueOf(contrast));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.CONTRAST, String.valueOf(contrast));
						break;
					case SCREEN_COLOR:
						int screenColor = (int) Float.parseFloat(value);
						dataConvert = Integer.toHexString(screenColor);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), dataConvert.getBytes(StandardCharsets.UTF_8), false, value);
						stats.put(group + LgLCDConstants.SCREEN_COLOR_VALUE, String.valueOf(screenColor));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.SCREEN_COLOR, String.valueOf(screenColor));
						break;
					case SHARPNESS:
						int sharpness = (int) Float.parseFloat(value);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), Integer.toHexString(sharpness).getBytes(StandardCharsets.UTF_8), false, value);
						stats.put(group + LgLCDConstants.SHARPNESS_VALUE, String.valueOf(sharpness));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.SHARPNESS, String.valueOf(sharpness));
						break;
					case TINT:
						String tint = EnumTypeHandler.getValueOfEnumByName(Tint.class, value);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), tint.getBytes(StandardCharsets.UTF_8), true, value);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TINT, tint);
						break;
					case ASPECT_RATIO:
						String aspectRatio = EnumTypeHandler.getValueOfEnumByName(AspectRatio.class, value);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), aspectRatio.getBytes(StandardCharsets.UTF_8), true, value);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.ASPECT_RATIO, value);
						break;
					case BRIGHTNESS_CONTROL:
						String brightnessSize = EnumTypeHandler.getValueOfEnumByName(BrightnessSize.class, value);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), brightnessSize.getBytes(StandardCharsets.UTF_8), true, value);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.BRIGHTNESS_CONTROL, value);
						break;
					case LANGUAGE:
						String language = EnumTypeHandler.getValueOfEnumByName(Language.class, value);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), language.getBytes(StandardCharsets.UTF_8), true, value);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.LANGUAGE, value);
						break;
					case SOUND_MODE:
						String soundMode = EnumTypeHandler.getValueOfEnumByName(SoundMode.class, value);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), soundMode.getBytes(StandardCharsets.UTF_8), true, value);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.SOUND_MODE, value);
						break;
					case PICTURE_MODE:
						String pictureMode = EnumTypeHandler.getValueOfEnumByName(PictureMode.class, value);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), pictureMode.getBytes(StandardCharsets.UTF_8), true, value);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.PICTURE_MODE, pictureMode);
						break;
					case POWER_ON_STATUS:
						String powerStatus = EnumTypeHandler.getValueOfEnumByName(PowerStatus.class, value);
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), powerStatus.getBytes(StandardCharsets.UTF_8), false, value);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.POWER_ON_STATUS, value);
						break;
					case NO_IR_POWER_OFF:
					case NO_SIGNAL_POWER_OFF:
						String powerValue = String.valueOf(LgLCDConstants.ZERO) + LgLCDConstants.ZERO;
						if (String.valueOf(LgLCDConstants.NUMBER_ONE).equals(value)) {
							powerValue = String.valueOf(LgLCDConstants.ZERO) + LgLCDConstants.NUMBER_ONE;
						}
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), powerValue.getBytes(StandardCharsets.UTF_8), false, value);
						powerValue = Integer.parseInt(powerValue) == LgLCDConstants.ZERO ? LgLCDConstants.OFF : LgLCDConstants.ON;
						if (lgControllingCommand.getName().equals(LgControllingCommand.NO_IR_POWER_OFF.getName())) {
							updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NO_IR_POWER_OFF, powerValue);
						} else {
							updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NO_SIGNAL_POWER_OFF, powerValue);
						}
						break;
					case REBOOT:
						String rebootValue = LgLCDConstants.REBOOT_VALUE;
						sendRequestToControlValue(lgControllingCommand.getCommandNames(), rebootValue.getBytes(StandardCharsets.UTF_8), false, rebootValue);
						break;
					default:
						logger.debug(String.format("Property name %s doesn't support", propertyKey));
				}
			}
			updateValueForTheControllableProperty(property, value, stats, advancedControllableProperties);
		} finally {
			reentrantLock.unlock();
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
		List<AdvancedControllableProperty> advancedControllableProperties = new ArrayList<>();
		Map<String, String> statistics = new HashMap<>();
		Map<String, String> controlStatistics = new HashMap<>();
		Map<String, String> dynamicStatistics = new HashMap<>();
		reentrantLock.lock();
		try {
			if (localExtendedStatistics == null) {
				localExtendedStatistics = new ExtendedStatistics();
			}
			if (!isEmergencyDelivery) {
				convertCacheLifetime();
				convertDelayTime();
				convertConfigTimeout();
				convertPollingInterval();
				failedMonitor.clear();
				convertConfigManagement();
				//Use thread to fetching the monitoring and controlling data if connected with the device successfully
				populateMonitoringAndControllingData();
				//destroy channel after collecting all device's information
				destroyChannel();
				if (!isFirstInit && currentGetMultipleInPollingInterval < pollingIntervalInIntValue) {
					if (localExtendedStatistics.getStatistics() != null) {
						populateAdapterMetadata(localExtendedStatistics.getStatistics());
					}
					return Collections.singletonList(localExtendedStatistics);
				}
				//We will check if the value of localCachedFailedMonitor is greater than or equal to the value of currentCommandIndex,
				// as we have 36 properties by default and if all options are enabled, then we have a total of 37 properties.
				if (localCachedFailedMonitor >= currentCommandIndex && currentGetMultipleInPollingInterval == pollingIntervalInIntValue) {
					//Handle the case where all properties receive an error response and the case where 2 connections run in parallel to the device
					ping();
					isFirstInit = false;
					statistics.put(LgLCDConstants.CONTROL_PROTOCOL_STATUS, LgLCDConstants.UNAVAILABLE);
					countControlUnavailable++;
					if (countControlUnavailable > currentCachingLifetime) {
						localCacheMapOfPropertyNameAndValue.clear();
						localCachingLifeTimeOfMap.clear();
					}
				} else {
					isFirstInit = true;
					populateMonitoringData(statistics, dynamicStatistics);
					if (isConfigManagement) {
						populateControllingData(controlStatistics, advancedControllableProperties);
						extendedStatistics.setControllableProperties(advancedControllableProperties);
						statistics.putAll(controlStatistics);
					} else {
						statistics.remove(LgLCDConstants.INPUT + LgLCDConstants.HASH + LgLCDConstants.SIGNAL);
					}
					//If failed for all monitoring data
					checkFailedCommand(statistics, advancedControllableProperties);
					extendedStatistics.setDynamicStatistics(dynamicStatistics);
					countControlUnavailable = 0;
				}
				populateAdapterMetadata(statistics);
				extendedStatistics.setStatistics(statistics);
				extendedStatistics.setControllableProperties(advancedControllableProperties);
				localExtendedStatistics = extendedStatistics;
			}
			isEmergencyDelivery = false;
		} finally {
			reentrantLock.unlock();
		}
		return Collections.singletonList(localExtendedStatistics);
	}

	/**
	 * Add the AdapterMetadata group (adapter version, build date, uptime) to the statistics
	 *
	 * @param statistics statistics to add the properties to
	 */
	private void populateAdapterMetadata(Map<String, String> statistics) {
		adapterMetadata.populate(statistics);
	}

	/**
	 * populate Priority input
	 *
	 * @param stats the stats are list of statistics
	 * @param advancedControllableProperties the advancedControllableProperties is advancedControllableProperties instance
	 * @param groupName the groupName instance in GroupName#Key
	 * @param currentPriority the currentPriority is current value of priority property
	 */
	private void populatePriorityInput(Map<String, String> stats, List<AdvancedControllableProperty> advancedControllableProperties, String groupName, String currentPriority) {
		String priorityInputUp = groupName + LgLCDConstants.PRIORITY_UP;
		String priorityInputDown = groupName + LgLCDConstants.PRIORITY_DOWN;
		stats.remove(priorityInputDown);
		advancedControllableProperties.removeIf(item -> item.getName().equals(priorityInputDown));

		stats.remove(priorityInputUp);
		advancedControllableProperties.removeIf(item -> item.getName().equals(priorityInputUp));

		String priorityInputStart = cacheMapOfPriorityInputAndValue.get(LgLCDConstants.PRIORITY + LgLCDConstants.NUMBER_ONE);
		String priorityInputEnd = cacheMapOfPriorityInputAndValue.get(LgLCDConstants.PRIORITY + cacheMapOfPriorityInputAndValue.size());
		if (StringUtils.isNullOrEmpty(priorityInputEnd)) {
			if (cacheMapOfPriorityInputAndValue.entrySet().stream().map(item -> item.getValue().equalsIgnoreCase(LgLCDConstants.PLAY_VIA_URL)).findFirst().isPresent()) {
				priorityInputEnd = cacheMapOfPriorityInputAndValue.get(LgLCDConstants.PRIORITY + (cacheMapOfPriorityInputAndValue.size() - 1));
			}
		}
		if (LgLCDConstants.NA.equals(currentPriority) || cacheMapOfPriorityInputAndValue.isEmpty()) {
			stats.put(groupName + LgLCDConstants.PRIORITY_UP, LgLCDConstants.NA);
			stats.put(groupName + LgLCDConstants.PRIORITY_DOWN, LgLCDConstants.NA);
			return;
		}
		if (!currentPriority.equals(priorityInputStart) && !currentPriority.equals(priorityInputEnd)) {
			stats.put(groupName + LgLCDConstants.PRIORITY_UP, LgLCDConstants.NA);
			advancedControllableProperties.add(createButton(groupName + LgLCDConstants.PRIORITY_UP, LgLCDConstants.UP, LgLCDConstants.PROCESSING, 0));

			stats.put(groupName + LgLCDConstants.PRIORITY_DOWN, LgLCDConstants.NA);
			advancedControllableProperties.add(createButton(groupName + LgLCDConstants.PRIORITY_DOWN, LgLCDConstants.DOWN, LgLCDConstants.PROCESSING, 0));
		} else if (!StringUtils.isNullOrEmpty(priorityInputEnd) && !currentPriority.equals(priorityInputEnd)) {
			stats.put(groupName + LgLCDConstants.PRIORITY_DOWN, LgLCDConstants.NA);
			advancedControllableProperties.add(createButton(groupName + LgLCDConstants.PRIORITY_DOWN, LgLCDConstants.DOWN, LgLCDConstants.PROCESSING, 0));
		} else {
			stats.put(groupName + LgLCDConstants.PRIORITY_UP, LgLCDConstants.NA);
			advancedControllableProperties.add(createButton(groupName + LgLCDConstants.PRIORITY_UP, LgLCDConstants.UP, LgLCDConstants.PROCESSING, 0));
		}
	}

	/**
	 * populate monitoring and controlling data
	 * using 2 thread to get monitoring and controlling data
	 * Thread 1 fetches device monitoring and controlling data of each property's
	 * Thread 2 manages the timeout for each command sent by Thread 1
	 *
	 * if the response time is greater than the default timeout => Close connection and update failedMonitor
	 */
	private void populateMonitoringAndControllingData() throws InterruptedException {
		List<LgControllingCommand> commands = Arrays.stream(LgControllingCommand.values()).filter(item -> item.isMonitorType() || item.isControlType()).collect(Collectors.toList());
		Future manageTimeOutWorkerThread;
		int range = 0;
		if (currentGetMultipleInPollingInterval == pollingIntervalInIntValue - 1) {
			range = commands.size();
		}
		if (currentGetMultipleInPollingInterval == pollingIntervalInIntValue) {
			devicesExecutionPool.clear();
			currentGetMultipleInPollingInterval = 0;
			localCachedFailedMonitor = 0;
			range = 0;
			currentCommandIndex = 0;
		}
		int intervalIndex = currentGetMultipleInPollingInterval * commands.size() / pollingIntervalInIntValue;
		if (range == 0) {
			range = (currentGetMultipleInPollingInterval + LgLCDConstants.NUMBER_ONE) * commands.size() / pollingIntervalInIntValue;
		}
		for (int i = intervalIndex; i < range; i++) {
			LgControllingCommand controllingCommand = commands.get(i);
			if (!isConfigManagement && controllingCommand.isControlType()) {
				continue;
			}
			if ((controllingCommand.isControlType() || controllingCommand.isMonitorType())) {
				commandNames param = getParamByCommandName(controllingCommand);
				//Count the number of requests in one polling cycle.
				currentCommandIndex++;
				if (param == null) {
					continue;
				}
				//Submit thread to fetch data
				devicesExecutionPool.add(fetchingDataExSer.submit(() -> {
					retrieveDataByCommandName(controllingCommand.getCommandNames(), param, controllingCommand);
				}));
				// The thread responsible for checking the ExecutorService waits until the defaultConfigTimeout period has elapsed.
				// If the Future is not completed at that point, the thread will cancel it
				manageTimeOutWorkerThread = timeoutManagementExSer.submit(() -> {
					int timeoutCount = 1;
					while (!devicesExecutionPool.get(devicesExecutionPool.size() - LgLCDConstants.ORDINAL_TO_INDEX_CONVERT_FACTOR).isDone() && timeoutCount <= defaultConfigTimeout) {
						try {
							Thread.sleep(100);

							// The thread waits until the controlProperty() method successfully controls.
							if (isEmergencyDelivery) {
								condition.wait();
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						timeoutCount++;
					}
					//If the Future is not completed after the defaultConfigTimeout =>  update the failedMonitor and destroy the connection.
					int lastIndex = devicesExecutionPool.size() - 1;
					if (!devicesExecutionPool.get(lastIndex).isDone()) {
						failedMonitor.add(controllingCommand.getName());
						destroyChannel();
						devicesExecutionPool.get(lastIndex).cancel(true);
					}
				});
				try {
					while (!manageTimeOutWorkerThread.isDone()) {
						Thread.sleep(100);
					}
					manageTimeOutWorkerThread.get();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		logger.debug("Get data success with getMultipleTime: " + currentGetMultipleInPollingInterval);
		currentGetMultipleInPollingInterval++;
		localCachedFailedMonitor = localCachedFailedMonitor + failedMonitor.size();
	}

	/**
	 * Get param request by command name
	 *
	 * @param commandName is the command names value
	 * @return commandNames is param value
	 */
	private commandNames getParamByCommandName(LgControllingCommand commandName) {
		commandNames param = commandNames.GET;
		if (LgControllingCommand.NATURAL_MODE.getName().equals(commandName.getName())) {
			String value = getValueByName(LgLCDConstants.TILE_MODE);
			if (LgLCDConstants.NA.equals(value) || !LgLCDConstants.ON.equals(value)) {
				return null;
			}
		} else if (LgControllingCommand.NATURAL_SIZE.getName().equals(commandName.getName())) {
			String value = getValueByName(LgLCDConstants.NATURAL_MODE);
			if (LgLCDConstants.NA.equals(value) || LgLCDConstants.NUMBER_ONE != Integer.parseInt(value)) {
				return null;
			}
			param = commandNames.NATURAL_SIZE_PARAM;
		} else if (LgControllingCommand.NETWORK_SETTING.getName().equals(commandName.getName())) {
			param = commandNames.NETWORK_SETTING_PARAM;
		} else if (LgControllingCommand.POWER_MANAGEMENT_MODE.getName().equals(commandName.getName())) {
			param = commandNames.POWER_MANAGEMENT_MODE_PARAM;
		} else if (LgControllingCommand.SYNC_STATUS.getName().equals(commandName.getName())) {
			param = commandNames.SYNC_STATUS_PARAM;
		}
		return param;
	}

	/**
	 * Check failed command when retrieving data
	 *
	 * @param statistics the statistics are list of statistics
	 * @param advancedControllableProperties the advancedControllableProperties is advancedControllableProperties instance
	 */
	private void checkFailedCommand(Map<String, String> statistics, List<AdvancedControllableProperty> advancedControllableProperties) {
		if (!failedMonitor.isEmpty()) {
			for (String value : failedMonitor) {
				Optional<Entry<String, Integer>> cachingCurrentValue = localCachingLifeTimeOfMap.entrySet().stream().filter(item -> item.getKey().equalsIgnoreCase(value)).findFirst();
				if (cachingCurrentValue.isPresent()) {
					int currentCachingLifetime = cachingCurrentValue.get().getValue();
					LgControllingCommand controllingCommand = LgControllingCommand.getCommandByName(value);
					if (currentCachingLifetime >= this.currentCachingLifetime) {
						localCachingLifeTimeOfMap.put(value.toLowerCase(Locale.ROOT), 0);
						switch (controllingCommand) {
							case NETWORK_SETTING:
								localCacheMapOfPropertyNameAndValue.remove(LgLCDConstants.IP_ADDRESS);
								localCacheMapOfPropertyNameAndValue.remove(LgLCDConstants.GATEWAY);
								localCacheMapOfPropertyNameAndValue.remove(LgLCDConstants.SUBNET_MASK);
								localCacheMapOfPropertyNameAndValue.remove(LgLCDConstants.DNS_SERVER);
								statistics.put(LgLCDConstants.GATEWAY, LgLCDConstants.NA);
								statistics.put(LgLCDConstants.SUBNET_MASK, LgLCDConstants.NA);
								statistics.put(LgLCDConstants.DNS_SERVER, LgLCDConstants.NA);
								statistics.put(LgLCDConstants.IP_ADDRESS, LgLCDConstants.NA);
								break;
							case TILE_MODE_SETTINGS:
								String groupName = LgLCDConstants.TILE_MODE_SETTINGS + LgLCDConstants.HASH;
								if (String.valueOf(LgLCDConstants.NUMBER_ONE).equalsIgnoreCase(statistics.get(groupName + LgLCDConstants.TILE_MODE))) {
									if (String.valueOf(LgLCDConstants.NUMBER_ONE).equalsIgnoreCase(statistics.get(groupName + LgLCDConstants.NATURAL_MODE))) {
										statistics.put(groupName + LgLCDConstants.NATURAL_SIZE, LgLCDConstants.NA);
										updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NATURAL_SIZE, LgLCDConstants.NA);
									}
									statistics.put(groupName + LgLCDConstants.NATURAL_MODE, LgLCDConstants.NA);
									updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NATURAL_MODE, LgLCDConstants.NA);
									advancedControllableProperties.removeIf(item -> item.getName().equals(groupName + LgLCDConstants.NATURAL_MODE));
									statistics.put(groupName + LgLCDConstants.TILE_MODE_ID, LgLCDConstants.NA);
								}
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TILE_MODE_COLUMN, LgLCDConstants.NA);
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TILE_MODE_ROW, LgLCDConstants.NA);
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TILE_MODE, LgLCDConstants.NA);
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TILE_MODE_ID, LgLCDConstants.NA);
								statistics.put(groupName + LgLCDConstants.TILE_MODE_COLUMN, LgLCDConstants.NA);
								statistics.put(groupName + LgLCDConstants.TILE_MODE_ROW, LgLCDConstants.NA);
								statistics.put(groupName + LgLCDConstants.TILE_MODE, LgLCDConstants.NA);
								advancedControllableProperties.removeIf(item -> item.getName().equals(groupName + LgLCDConstants.TILE_MODE));
								break;
							case NATURAL_MODE:
								groupName = LgLCDConstants.TILE_MODE_SETTINGS + LgLCDConstants.HASH;
								if (String.valueOf(LgLCDConstants.NUMBER_ONE).equalsIgnoreCase(statistics.get(groupName + LgLCDConstants.NATURAL_MODE))) {
									statistics.put(groupName + LgLCDConstants.NATURAL_SIZE, LgLCDConstants.NA);
									updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NATURAL_SIZE, LgLCDConstants.NA);
								}
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NATURAL_MODE, LgLCDConstants.NA);
								statistics.put(groupName + LgLCDConstants.NATURAL_MODE, LgLCDConstants.NA);
								advancedControllableProperties.removeIf(item -> item.getName().equals(groupName + LgLCDConstants.NATURAL_MODE));
								break;
							case DATE:
							case TIME:
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.DATE, LgLCDConstants.NA);
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TIME, LgLCDConstants.NA);
								statistics.put(LgLCDConstants.DATE_TIME, LgLCDConstants.NA);
								localCacheMapOfPropertyNameAndValue.remove(value);
								break;
							default:
								Entry<String, String> property = statistics.entrySet().stream().filter((item) -> {
									String key = item.getKey();
									String[] group = key.split(LgLCDConstants.HASH);
									String propertyName = group[0];
									if (key.contains(LgLCDConstants.HASH)) {
										propertyName = group[1];
									}
									return propertyName.equals(value);
								}).findFirst().orElse(null);
								if (property != null) {
									statistics.put(property.getKey(), LgLCDConstants.NA);
									advancedControllableProperties.removeIf(item -> item.getName().equals(property.getKey()));
									localCacheMapOfPropertyNameAndValue.remove(value);
								}
								break;
						}
					} else {
						currentCachingLifetime = currentCachingLifetime + 1;
						localCachingLifeTimeOfMap.replace(cachingCurrentValue.get().getKey(), currentCachingLifetime);
					}
				} else {
					localCachingLifeTimeOfMap.put(value.toLowerCase(Locale.ROOT), 1);
				}
			}
		}
	}

	/**
	 * This method is used to convert from api value string to ui value in integer
	 *
	 * @param apiCurrentValue current api value of property
	 * @param apiMaxValue max api value of property
	 * @param apiMinValue min api value of property
	 * @return float ui value
	 */
	private float convertFromApiValueToUIValue(String apiCurrentValue, int apiMaxValue, int apiMinValue) {
		if (StringUtils.isNotNullOrEmpty(apiCurrentValue) && !LgLCDConstants.NA.equals(apiCurrentValue)) {
			int a = Integer.parseInt(apiCurrentValue) - apiMinValue;
			int b = apiMaxValue - apiMinValue;
			return a * (LgLCDConstants.COLOR_TEMPERATURE_UI_MAX_VALUE - LgLCDConstants.COLOR_TEMPERATURE_UI_MIN_VALUE) / b + LgLCDConstants.COLOR_TEMPERATURE_UI_MIN_VALUE;
		}
		return 0f;
	}

	/**
	 * This method is used to convert from ui value in integer to api hex string
	 *
	 * @param currentValue current ui value of property
	 * @param maxValue max api value of property
	 * @param minValue min api value of property
	 * @return Float api current value
	 */
	private float convertFromUIValueToApiValue(String currentValue, int maxValue, int minValue) {
		if (StringUtils.isNotNullOrEmpty(currentValue) && !LgLCDConstants.NA.equals(currentValue)) {
			int a = Integer.parseInt(currentValue) - minValue;
			int b = maxValue - minValue;
			return a * (LgLCDConstants.COLOR_TEMPERATURE_MAX_VALUE - LgLCDConstants.COLOR_TEMPERATURE_MIN_VALUE) / b + LgLCDConstants.COLOR_TEMPERATURE_MIN_VALUE;
		}
		return 0f;
	}

	/**
	 * Update the value for the control metric
	 *
	 * @param property is name of the metric
	 * @param value the value is value of properties
	 * @param extendedStatistics list statistics property
	 * @param advancedControllableProperties the advancedControllableProperties is list AdvancedControllableProperties
	 */
	private void updateValueForTheControllableProperty(String property, String value, Map<String, String> extendedStatistics, List<AdvancedControllableProperty> advancedControllableProperties) {
		if (!advancedControllableProperties.isEmpty()) {
			for (AdvancedControllableProperty advancedControllableProperty : advancedControllableProperties) {
				if (advancedControllableProperty.getName().equals(property)) {
					// buttons have no state: they always report N/A, not the pressed value
					String newValue = advancedControllableProperty.getType() instanceof AdvancedControllableProperty.Button ? LgLCDConstants.NA : value;
					extendedStatistics.put(property, newValue);
					advancedControllableProperty.setValue(newValue);
					break;
				}
			}
		}
	}

	/**
	 * Control property name by value
	 *
	 * @param command the command is command to send the request
	 * @param param the param is parameter of the request
	 * @param isDropdownControl whether a particular control is a dropdown control or not
	 * @param value the value is value of property
	 */
	private void sendRequestToControlValue(commandNames command, byte[] param, boolean isDropdownControl, String value) {
		try {
			byte[] response = send(LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(command), param));
			String result = digestResponse(response, command).toString();
			if (LgLCDConstants.NA.equals(result)) {
				throw new IllegalArgumentException("The response NG reply ");
			}
		} catch (Exception e) {
			if (isDropdownControl) {
				throw new IllegalArgumentException(
						String.format("The property name %s is not supported. The current model does not support control with the value %s, and the device has responded with an error.", command.name(), value),
						e);
			}
			throw new IllegalArgumentException(String.format("Can't control property %s. The device has responded with an error.", command.name()), e);
		}
	}

	/**
	 * Populate controlling data
	 *
	 * @param controlStatistics the controlStatistics are list of statistics
	 * @param advancedControllableProperties the advancedControllableProperties is advancedControllableProperties instance
	 */
	private void populateControllingData(Map<String, String> controlStatistics, List<AdvancedControllableProperty> advancedControllableProperties) {
		retrieveFailOverGroupValue(controlStatistics, advancedControllableProperties);
		retrieveDisplayAndSoundGroupValue(controlStatistics, advancedControllableProperties);
		retrieveTileModeGroupValue(controlStatistics, advancedControllableProperties);
		for (LgControllingCommand lgControllingCommand : LgControllingCommand.values()) {
			populateDisplayPropertyGroup(lgControllingCommand, controlStatistics, advancedControllableProperties);
		}
	}

	/**
	 * check Control Property Before Add New Property
	 *
	 * @param advancedControllableProperty the advancedControllableProperty is AdvancedControllableProperty instance
	 * @param advancedControllableProperties the advancedControllableProperties is advancedControllableProperties instance
	 */
	private void checkControlPropertyBeforeAddNewProperty(AdvancedControllableProperty advancedControllableProperty, List<AdvancedControllableProperty> advancedControllableProperties) {
		if (advancedControllableProperty != null) {
			advancedControllableProperties.add(advancedControllableProperty);
		}
	}

	/**
	 * Populate controlling property
	 *
	 * @param lgControllingCommand the lgControllingCommand is LgControllingCommand enum instance
	 * @param controlStatistics the controlStatistics are list of statistics
	 * @param advancedControllableProperties the advancedControllableProperties is advancedControllableProperties instance
	 */
	private void populateDisplayPropertyGroup(LgControllingCommand lgControllingCommand, Map<String, String> controlStatistics,
			List<AdvancedControllableProperty> advancedControllableProperties) {
		String displayGroupName = LgLCDConstants.DISPLAY + LgLCDConstants.HASH;
		String soundGroupName = LgLCDConstants.SOUND + LgLCDConstants.HASH;
		String powerManagementGroupName = LgLCDConstants.POWER_MANAGEMENT + LgLCDConstants.HASH;
		String value;
		switch (lgControllingCommand) {
			case POWER:
				value = getValueByName(LgLCDConstants.POWER);
				if (!LgLCDConstants.NA.equals(value)) {
					value = String.valueOf(LgLCDConstants.ON.equalsIgnoreCase(value) ? 1 : 0);
				}
				AdvancedControllableProperty controlPower = controlSwitch(controlStatistics, LgLCDConstants.POWER, value, LgLCDConstants.OFF, LgLCDConstants.ON);
				checkControlPropertyBeforeAddNewProperty(controlPower, advancedControllableProperties);

				controlStatistics.put(LgLCDConstants.REBOOT, LgLCDConstants.NA);
				advancedControllableProperties.add(createButton(LgLCDConstants.REBOOT, LgLCDConstants.REBOOT, LgLCDConstants.PROCESSING, 0));
				break;
			case ASPECT_RATIO:
				value = getValueByName(LgLCDConstants.ASPECT_RATIO);
				String[] aspectRatioDropdown = EnumTypeHandler.getEnumNames(AspectRatio.class);
				AdvancedControllableProperty aspectRatioControl = controlDropdown(controlStatistics, aspectRatioDropdown, displayGroupName + LgLCDConstants.ASPECT_RATIO, value);
				checkControlPropertyBeforeAddNewProperty(aspectRatioControl, advancedControllableProperties);
				break;
			case BRIGHTNESS_CONTROL:
				value = getValueByName(LgLCDConstants.BRIGHTNESS_CONTROL);
				String[] brightnessSizeDropdown = EnumTypeHandler.getEnumNames(BrightnessSize.class);
				AdvancedControllableProperty brightnessSizeControl = controlDropdown(controlStatistics, brightnessSizeDropdown, displayGroupName + LgLCDConstants.BRIGHTNESS_CONTROL,
						value);
				checkControlPropertyBeforeAddNewProperty(brightnessSizeControl, advancedControllableProperties);
				break;
			case CONTRAST:
				value = getValueByName(LgLCDConstants.CONTRAST);
				getDefaultValueForNullValue(value, controlStatistics, displayGroupName + LgLCDConstants.CONTRAST_VALUE);
				AdvancedControllableProperty controlContrast = createControlSlider(displayGroupName + LgLCDConstants.CONTRAST, value, controlStatistics, String.valueOf(LgLCDConstants.ZERO),
						String.valueOf(LgLCDConstants.MAX_RANGE_CONTRAST));
				checkControlPropertyBeforeAddNewProperty(controlContrast, advancedControllableProperties);
				break;
			case PICTURE_MODE:
				value = getValueByName(LgLCDConstants.PICTURE_MODE);
				String[] pictureModeDropdown = EnumTypeHandler.getEnumNames(PictureMode.class);
				AdvancedControllableProperty pictureModeControl = controlDropdown(controlStatistics, pictureModeDropdown, displayGroupName + LgLCDConstants.PICTURE_MODE, value);
				checkControlPropertyBeforeAddNewProperty(pictureModeControl, advancedControllableProperties);
				break;
			case BRIGHTNESS:
				value = getValueByName(LgLCDConstants.BRIGHTNESS);
				getDefaultValueForNullValue(value, controlStatistics, displayGroupName + LgLCDConstants.BRIGHTNESS_VALUE);
				AdvancedControllableProperty controlBrightness = createControlSlider(displayGroupName + LgLCDConstants.BRIGHTNESS, value, controlStatistics, String.valueOf(LgLCDConstants.ZERO),
						String.valueOf(LgLCDConstants.MAX_RANGE_BRIGHTNESS));
				checkControlPropertyBeforeAddNewProperty(controlBrightness, advancedControllableProperties);
				break;
			case SHARPNESS:
				value = getValueByName(LgLCDConstants.SHARPNESS);
				getDefaultValueForNullValue(value, controlStatistics, displayGroupName + LgLCDConstants.SHARPNESS_VALUE);
				AdvancedControllableProperty controlSharpness = createControlSlider(displayGroupName + LgLCDConstants.SHARPNESS, value, controlStatistics, String.valueOf(LgLCDConstants.ZERO),
						String.valueOf(LgLCDConstants.MAX_RANGE_SHARPNESS));
				checkControlPropertyBeforeAddNewProperty(controlSharpness, advancedControllableProperties);
				break;
			case SCREEN_COLOR:
				value = getValueByName(LgLCDConstants.SCREEN_COLOR);
				getDefaultValueForNullValue(value, controlStatistics, displayGroupName + LgLCDConstants.SCREEN_COLOR_VALUE);
				AdvancedControllableProperty controlScreenColor = createControlSlider(displayGroupName + LgLCDConstants.SCREEN_COLOR, value, controlStatistics, String.valueOf(LgLCDConstants.ZERO),
						String.valueOf(LgLCDConstants.MAX_RANGE_SCREEN_COLOR));
				checkControlPropertyBeforeAddNewProperty(controlScreenColor, advancedControllableProperties);
				break;
			case TINT:
				value = getValueByName(LgLCDConstants.TINT);
				String[] tintDropdown = EnumTypeHandler.getEnumNames(Tint.class);
				String tintValue = EnumTypeHandler.getNameEnumByValue(Tint.class, value);
				AdvancedControllableProperty controlTint = controlDropdown(controlStatistics, tintDropdown, displayGroupName + LgLCDConstants.TINT, tintValue);
				checkControlPropertyBeforeAddNewProperty(controlTint, advancedControllableProperties);
				break;
			case COLOR_TEMPERATURE:
				value = getValueByName(LgLCDConstants.COLOR_TEMPERATURE);
				Float colorTemperatureValue = convertFromApiValueToUIValue(value, LgLCDConstants.COLOR_TEMPERATURE_MAX_VALUE, LgLCDConstants.COLOR_TEMPERATURE_MIN_VALUE);
				if (colorTemperatureValue != 0f) {
					value = String.valueOf((int) Float.parseFloat(String.valueOf(colorTemperatureValue)));
				}
				getDefaultValueForNullValue(value, controlStatistics, displayGroupName + LgLCDConstants.COLOR_TEMPERATURE_VALUE);
				AdvancedControllableProperty controlColorTemperature = createControlSlider(displayGroupName + LgLCDConstants.COLOR_TEMPERATURE, value, controlStatistics,
						String.valueOf(LgLCDConstants.MIN_RANGE_COLOR_TEMPERATURE),
						String.valueOf(LgLCDConstants.MAX_RANGE_COLOR_TEMPERATURE));
				checkControlPropertyBeforeAddNewProperty(controlColorTemperature, advancedControllableProperties);
				break;
			case BALANCE:
				value = getValueByName(LgLCDConstants.BALANCE);
				String[] balanceDropdown = EnumTypeHandler.getEnumNames(Balance.class);
				String balanceValue = EnumTypeHandler.getNameEnumByValue(Balance.class, value);
				AdvancedControllableProperty controlBalance = controlDropdown(controlStatistics, balanceDropdown, soundGroupName + LgLCDConstants.BALANCE, balanceValue);
				checkControlPropertyBeforeAddNewProperty(controlBalance, advancedControllableProperties);
				break;
			case SOUND_MODE:
				value = getValueByName(LgLCDConstants.SOUND_MODE);
				String[] soundModeDropdown = EnumTypeHandler.getEnumNames(SoundMode.class);
				AdvancedControllableProperty soundModeControl = controlDropdown(controlStatistics, soundModeDropdown, soundGroupName + LgLCDConstants.SOUND_MODE, value);
				checkControlPropertyBeforeAddNewProperty(soundModeControl, advancedControllableProperties);
				break;
			case LANGUAGE:
				value = getValueByName(LgLCDConstants.LANGUAGE);
				String[] languageDropdown = EnumTypeHandler.getEnumNames(Language.class);
				AdvancedControllableProperty languageControl = controlDropdown(controlStatistics, languageDropdown, LgLCDConstants.LANGUAGE, value);
				checkControlPropertyBeforeAddNewProperty(languageControl, advancedControllableProperties);
				break;
			case POWER_ON_STATUS:
				value = getValueByName(LgLCDConstants.POWER_ON_STATUS);
				String[] powerDropdown = EnumTypeHandler.getEnumNames(PowerStatus.class);
				AdvancedControllableProperty powerControl = controlDropdown(controlStatistics, powerDropdown, powerManagementGroupName + LgLCDConstants.POWER_ON_STATUS, value);
				checkControlPropertyBeforeAddNewProperty(powerControl, advancedControllableProperties);
				break;
			case NO_SIGNAL_POWER_OFF:
				value = getValueByName(LgLCDConstants.NO_SIGNAL_POWER_OFF);
				if (!LgLCDConstants.NA.equals(value)) {
					value = String.valueOf(LgLCDConstants.ON.equalsIgnoreCase(value) ? LgLCDConstants.NUMBER_ONE : LgLCDConstants.ZERO);
				}
				AdvancedControllableProperty controlNoSignalPower = controlSwitch(controlStatistics, powerManagementGroupName + LgLCDConstants.NO_SIGNAL_POWER_OFF, value,
						LgLCDConstants.OFF,
						LgLCDConstants.ON);
				checkControlPropertyBeforeAddNewProperty(controlNoSignalPower, advancedControllableProperties);
				break;
			case NO_IR_POWER_OFF:
				value = getValueByName(LgLCDConstants.NO_IR_POWER_OFF);
				if (!LgLCDConstants.NA.equalsIgnoreCase(value)) {
					value = String.valueOf(LgLCDConstants.ON.equalsIgnoreCase(value) ? LgLCDConstants.NUMBER_ONE : LgLCDConstants.ZERO);
				}
				AdvancedControllableProperty controlNoIRPower = controlSwitch(controlStatistics, powerManagementGroupName + LgLCDConstants.NO_IR_POWER_OFF, value, LgLCDConstants.OFF,
						LgLCDConstants.ON);
				checkControlPropertyBeforeAddNewProperty(controlNoIRPower, advancedControllableProperties);
				break;
			default:
				logger.debug("the command name isn't supported" + lgControllingCommand.getName());
				break;
		}
	}

	/**
	 * Get default value for null value
	 *
	 * @param value the value is value of property
	 * @param stats the stats are list of statistics
	 * @param property the property is property name
	 */
	private void getDefaultValueForNullValue(String value, Map<String, String> stats, String property) {
		if (!StringUtils.isNullOrEmpty(value) && !LgLCDConstants.NA.equals(value)) {
			stats.put(property, value);
		}
	}

	/**
	 * Populate monitoring data
	 *
	 * @param statistics the statistics are list of statistics
	 * @param dynamicStatistics the dynamicStatistics are list of dynamicStatistics
	 */
	private void populateMonitoringData(Map<String, String> statistics, Map<String, String> dynamicStatistics) {
		//The flow code is handled in the previous version
		String inputGroupName = LgLCDConstants.INPUT + LgLCDConstants.HASH;
		String signal = getValueByName(LgLCDConstants.SIGNAL);
		if (LgLCDConstants.NA.equals(signal)) {
			signal = syncStatusNames.NO_SYNC.name();
		}
		statistics.put(LgLCDConstants.SIGNAL, signal);
		statistics.put(inputGroupName + LgLCDConstants.SIGNAL, signal);
		String inputSignal = getValueByName(LgLCDConstants.INPUT_SELECT);
		statistics.put(LgLCDConstants.INPUT_SELECT, inputSignal);

		String fan = getValueByName(LgLCDConstants.FAN);
		if (LgLCDConstants.NA.equals(fan)) {
			fan = fanStatusNames.NO_FAN.name();
		}
		statistics.put(LgLCDConstants.FAN, fan);

		String temperatureValue = getValueByName(LgLCDConstants.TEMPERATURE);
		if (!historicalProperties.isEmpty() && historicalProperties.contains(LgLCDConstants.TEMPERATURE)) {
			dynamicStatistics.put(LgLCDConstants.TEMPERATURE, temperatureValue);
		} else {
			statistics.put(LgLCDConstants.TEMPERATURE, temperatureValue);
		}
		//new feature retrieve device dashboard
		String software = getValueByName(LgLCDConstants.SOFTWARE_VERSION);
		String failover = getValueByName(LgLCDConstants.FAILOVER_MODE);
		String tileMode = getValueByName(LgLCDConstants.TILE_MODE);
		String serialNumber = getValueByName(LgLCDConstants.SERIAL_NUMBER);
		String standbyMode = getValueByName(LgLCDConstants.DISPLAY_STAND_BY_MODE);
		String date = getValueByName(LgLCDConstants.DATE);
		String time = getValueByName(LgLCDConstants.TIME);

		String dateTimeValue = String.format("%s %s", date, time);
		if (LgLCDConstants.NA.equals(date) || LgLCDConstants.NA.equals(time)) {
			dateTimeValue = LgLCDConstants.NA;
		}
		if (!LgLCDConstants.OFF.equals(standbyMode) && !LgLCDConstants.NA.equals(standbyMode)) {
			standbyMode = LgLCDConstants.ON;
		}
		statistics.put(LgLCDConstants.DATE_TIME, dateTimeValue);
		statistics.put(LgLCDConstants.FAILOVER_MODE, failover);
		statistics.put(LgLCDConstants.SOFTWARE_VERSION, software);
		statistics.put(LgLCDConstants.TILE_MODE, tileMode);
		statistics.put(LgLCDConstants.SERIAL_NUMBER, serialNumber);
		statistics.put(LgLCDConstants.DISPLAY_STAND_BY_MODE, standbyMode);

		//populate Network information
		String ipAddress = getValueByName(LgLCDConstants.IP_ADDRESS);
		String gateway = getValueByName(LgLCDConstants.GATEWAY);
		String subnetMask = getValueByName(LgLCDConstants.SUBNET_MASK);
		String dnsServer = getValueByName(LgLCDConstants.DNS_SERVER);
		statistics.put(LgLCDConstants.GATEWAY, gateway);
		statistics.put(LgLCDConstants.SUBNET_MASK, subnetMask);
		statistics.put(LgLCDConstants.DNS_SERVER, dnsServer);
		statistics.put(LgLCDConstants.IP_ADDRESS, ipAddress);
	}

	/**
	 * Retrieve tile mode group value
	 *
	 * @param controlStatistics the controlStatistics are list of statistics
	 * @param advancedControllableProperties the advancedControllableProperties is advancedControllableProperties instance
	 */
	private void retrieveTileModeGroupValue(Map<String, String> controlStatistics, List<AdvancedControllableProperty> advancedControllableProperties) {
		String groupName = LgLCDConstants.TILE_MODE_SETTINGS + LgLCDConstants.HASH;
		//populate tile settings
		String tileMode = getValueByName(LgLCDConstants.TILE_MODE);
		String tileModeValue = LgLCDConstants.NA;
		if (!LgLCDConstants.NA.equals(tileMode)) {
			tileModeValue = String.valueOf(LgLCDConstants.ON.equalsIgnoreCase(tileMode) ? 1 : 0);
		}
		AdvancedControllableProperty controlTileMode = controlSwitch(controlStatistics, groupName + LgLCDConstants.TILE_MODE, tileModeValue, LgLCDConstants.OFF, LgLCDConstants.ON);
		checkControlPropertyBeforeAddNewProperty(controlTileMode, advancedControllableProperties);

		controlStatistics.put(groupName + LgLCDConstants.TILE_MODE_COLUMN, getValueByName(LgLCDConstants.TILE_MODE_COLUMN));
		controlStatistics.put(groupName + LgLCDConstants.TILE_MODE_ROW, getValueByName(LgLCDConstants.TILE_MODE_ROW));

		//NaturalMode
		if (LgLCDConstants.ON.equals(tileMode)) {

			//Retrieve Tile ID
			String tileModeID = getValueByName(LgLCDConstants.TILE_MODE_ID);
			if (!LgLCDConstants.NA.equals(tileModeID)) {
				tileModeID = String.valueOf(Integer.parseInt(tileModeID));
			}
			controlStatistics.put(groupName + LgLCDConstants.TILE_MODE_ID, tileModeID);
			String naturalMode = getValueByName(LgLCDConstants.NATURAL_MODE);
			if (!LgLCDConstants.NA.equals(naturalMode)) {
				naturalMode = String.valueOf(LgLCDConstants.ZERO == Integer.parseInt(naturalMode) ? 0 : 1);
			}
			AdvancedControllableProperty controlNaturalMode = controlSwitch(controlStatistics, groupName + LgLCDConstants.NATURAL_MODE, naturalMode, LgLCDConstants.OFF, LgLCDConstants.ON);
			checkControlPropertyBeforeAddNewProperty(controlNaturalMode, advancedControllableProperties);
			if (String.valueOf(LgLCDConstants.NUMBER_ONE).equals(naturalMode)) {
				controlStatistics.put(groupName + LgLCDConstants.NATURAL_SIZE, getValueByName(LgLCDConstants.NATURAL_SIZE));
			}
		}
	}

	/**
	 * Retrieve fail over group value
	 *
	 * @param controlStatistics the controlStatistics are list of statistics
	 * @param advancedControllableProperties the advancedControllableProperties is advancedControllableProperties instance
	 */
	private void retrieveFailOverGroupValue(Map<String, String> controlStatistics, List<AdvancedControllableProperty> advancedControllableProperties) {
		String groupName = LgLCDConstants.FAILOVER + LgLCDConstants.HASH;
		String failOver = getValueByName(LgLCDConstants.FAILOVER_MODE);
		int failOverValue = LgLCDConstants.NUMBER_ONE;
		if (LgLCDConstants.NA.equals(failOver)) {
			controlStatistics.put(groupName + LgLCDConstants.INPUT_PRIORITY, failOver);
			return;
		}
		if (LgLCDConstants.OFF.equalsIgnoreCase(failOver)) {
			failOverValue = LgLCDConstants.ZERO;
		} else if (LgLCDConstants.AUTO.equalsIgnoreCase(failOver)) {
			AdvancedControllableProperty controlInputPriority = controlSwitch(controlStatistics, groupName + LgLCDConstants.INPUT_PRIORITY, String.valueOf(LgLCDConstants.ZERO), LgLCDConstants.AUTO,
					LgLCDConstants.MANUAL);
			checkControlPropertyBeforeAddNewProperty(controlInputPriority, advancedControllableProperties);
		} else {
			// failover is Manual
			AdvancedControllableProperty controlInputPriority = controlSwitch(controlStatistics, groupName + LgLCDConstants.INPUT_PRIORITY, String.valueOf(LgLCDConstants.NUMBER_ONE), LgLCDConstants.AUTO,
					LgLCDConstants.MANUAL);
			checkControlPropertyBeforeAddNewProperty(controlInputPriority, advancedControllableProperties);
			for (Entry<String, String> entry : cacheMapOfPriorityInputAndValue.entrySet()) {
				if (LgLCDConstants.PLAY_VIA_URL.equalsIgnoreCase(entry.getValue())) {
					continue;
				}
				controlStatistics.put(groupName + entry.getKey(), entry.getValue());
			}
			String[] inputSelected = cacheMapOfPriorityInputAndValue.values().stream().filter(item -> !item.equalsIgnoreCase(LgLCDConstants.PLAY_VIA_URL)).collect(Collectors.toList())
					.toArray(new String[0]);
			String priorityInput = getValueByName(LgLCDConstants.PRIORITY_INPUT);
			if (LgLCDConstants.NA.equals(priorityInput)) {
				Optional<Entry<String, String>> priorityInputOption = cacheMapOfPriorityInputAndValue.entrySet().stream().filter(item -> !item.getValue().equalsIgnoreCase(LgLCDConstants.PLAY_VIA_URL))
						.findFirst();
				if (priorityInputOption.isPresent()) {
					priorityInput = priorityInputOption.get().getValue();
				}
				localCacheMapOfPropertyNameAndValue.put(LgLCDConstants.PRIORITY_INPUT, priorityInput);
			}
			populatePriorityInput(controlStatistics, advancedControllableProperties, groupName, priorityInput);
			AdvancedControllableProperty controlInputSource = controlDropdown(controlStatistics, inputSelected, groupName + LgLCDConstants.PRIORITY_INPUT, priorityInput);
			checkControlPropertyBeforeAddNewProperty(controlInputSource, advancedControllableProperties);
		}
		AdvancedControllableProperty controlFailover = controlSwitch(controlStatistics, groupName + LgLCDConstants.FAILOVER_MODE, String.valueOf(failOverValue), LgLCDConstants.OFF, LgLCDConstants.ON);
		checkControlPropertyBeforeAddNewProperty(controlFailover, advancedControllableProperties);
	}

	/**
	 * Retrieve display and sound group value
	 *
	 * @param statistics the statistics are list of statistics
	 * @param advancedControllableProperties the advancedControllableProperties is advancedControllableProperties instance
	 */
	private void retrieveDisplayAndSoundGroupValue(Map<String, String> statistics, List<AdvancedControllableProperty> advancedControllableProperties) {
		String displayGroupName = LgLCDConstants.DISPLAY + LgLCDConstants.HASH;
		String soundGroupName = LgLCDConstants.SOUND + LgLCDConstants.HASH;
		String inputGroupName = LgLCDConstants.INPUT + LgLCDConstants.HASH;
		String powerManagementGroupName = LgLCDConstants.POWER_MANAGEMENT + LgLCDConstants.HASH;
		String backlight = getValueByName(LgLCDConstants.BACKLIGHT);
		String mute = getValueByName(LgLCDConstants.MUTE);
		String volume = getValueByName(LgLCDConstants.VOLUME);

		getDefaultValueForNullValue(backlight, statistics, displayGroupName + LgLCDConstants.BACKLIGHT_VALUE);
		AdvancedControllableProperty controlBacklight = createControlSlider(displayGroupName + LgLCDConstants.BACKLIGHT, backlight, statistics, String.valueOf(LgLCDConstants.ZERO),
				String.valueOf(LgLCDConstants.MAX_RANGE_BACKLIGHT));
		checkControlPropertyBeforeAddNewProperty(controlBacklight, advancedControllableProperties);

		if (!LgLCDConstants.NA.equals(mute)) {
			mute = String.valueOf(Integer.parseInt(mute) == 0 ? LgLCDConstants.NUMBER_ONE : LgLCDConstants.ZERO);
		}
		AdvancedControllableProperty controlMute = controlSwitch(statistics, soundGroupName + LgLCDConstants.MUTE, mute, LgLCDConstants.OFF, LgLCDConstants.ON);
		checkControlPropertyBeforeAddNewProperty(controlMute, advancedControllableProperties);

		getDefaultValueForNullValue(volume, statistics, soundGroupName + LgLCDConstants.VOLUME_VALUE);
		AdvancedControllableProperty controlVolume = createControlSlider(soundGroupName + LgLCDConstants.VOLUME, volume, statistics, String.valueOf(LgLCDConstants.ZERO),
				String.valueOf(LgLCDConstants.MAX_RANGE_VOLUME));
		checkControlPropertyBeforeAddNewProperty(controlVolume, advancedControllableProperties);

		String inputSourceValue = getValueByName(LgLCDConstants.INPUT_SELECT);
		if (!LgLCDConstants.NA.equals(inputSourceValue)) {
			cacheMapOfPriorityInputAndValue.put(LgLCDConstants.PLAY_VIA_URL, LgLCDConstants.PLAY_VIA_URL);
			String[] inputDropdown = cacheMapOfPriorityInputAndValue.values().stream().sorted().collect(Collectors.toList()).toArray(new String[0]);
			AdvancedControllableProperty controlInputSource = controlDropdown(statistics, inputDropdown, inputGroupName + LgLCDConstants.INPUT_SELECT, inputSourceValue);
			checkControlPropertyBeforeAddNewProperty(controlInputSource, advancedControllableProperties);
			statistics.put(LgLCDConstants.INPUT_SELECT, inputSourceValue);
		} else {
			statistics.put(inputGroupName + LgLCDConstants.INPUT_SELECT, LgLCDConstants.NA);
			statistics.put(LgLCDConstants.INPUT_SELECT, LgLCDConstants.NA);
		}
		String[] pmdDropdown = EnumTypeHandler.getEnumNames(PowerManagement.class);
		AdvancedControllableProperty controlPMD = controlDropdown(statistics, pmdDropdown, powerManagementGroupName + LgLCDConstants.DISPLAY_STAND_BY_MODE,
				getValueByName(LgLCDConstants.DISPLAY_STAND_BY_MODE));
		checkControlPropertyBeforeAddNewProperty(controlPMD, advancedControllableProperties);

		String pmdModeValue = getValueByName(LgLCDConstants.POWER_MANAGEMENT_MODE);

		String[] pmdModeDropdown = EnumTypeHandler.getEnumNames(PowerManagementModeEnum.class);
		AdvancedControllableProperty controlPMDMode = controlDropdown(statistics, pmdModeDropdown, powerManagementGroupName + LgLCDConstants.POWER_MANAGEMENT_MODE, pmdModeValue);
		checkControlPropertyBeforeAddNewProperty(controlPMDMode, advancedControllableProperties);
	}

	/**
	 * Get value of property by name
	 *
	 * @param name the name is name if property
	 * @return String is value of property or NA if the value is null or N/A
	 */
	private String getValueByName(String name) {
		String value = localCacheMapOfPropertyNameAndValue.get(name);
		if (StringUtils.isNullOrEmpty(value) || LgLCDConstants.NA.equals(value)) {
			return LgLCDConstants.NA;
		}
		return value;
	}

	/**
	 * Retrieve data by command name
	 *
	 * @param command the command is command to send the request get the data
	 * @param param the param is param to send the request get the data
	 * @return String is data response from the device or None if response fail
	 */
	private String retrieveDataByCommandName(commandNames command, commandNames param, LgControllingCommand lgControllingCommand) {
		try {
			byte[] response = send(LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(command), LgLCDConstants.commands.get(param)));
			return digestResponse(response, command).toString();
		} catch (Exception ce) {
			failedMonitor.add(lgControllingCommand.getName());
			return LgLCDConstants.NA;
		}
	}

	/**
	 * Control power on
	 */
	protected void powerON() {
		try {
			byte[] response = send(
					LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(LgLCDConstants.commandNames.POWER), LgLCDConstants.powerStatus.get(LgLCDConstants.powerStatusNames.ON)));

			digestResponse(response, LgLCDConstants.commandNames.POWER);
			updateCachedDeviceData(cacheMapOfPriorityInputAndValue, LgLCDConstants.POWER, LgLCDConstants.ON);
		} catch (Exception e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("error during power OFF send", e);
			}
		}
	}

	/**
	 * Control power off
	 */
	protected void powerOFF() {
		try {
			byte[] response = send(
					LgLCDUtils.buildSendString((byte) monitorID, LgLCDConstants.commands.get(LgLCDConstants.commandNames.POWER), LgLCDConstants.powerStatus.get(LgLCDConstants.powerStatusNames.OFF)));

			digestResponse(response, LgLCDConstants.commandNames.POWER);
			updateCachedDeviceData(cacheMapOfPriorityInputAndValue, LgLCDConstants.POWER, LgLCDConstants.OFF);
		} catch (Exception e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("error during power ON send", e);
			}
		}
	}

	/**
	 * Update cache device data
	 *
	 * @param cacheMapOfPropertyNameAndValue the cacheMapOfPropertyNameAndValue are map key and value of it
	 * @param property the key is property name
	 * @param value the value is String value
	 */
	private void updateCachedDeviceData(Map<String, String> cacheMapOfPropertyNameAndValue, String property, String value) {
		cacheMapOfPropertyNameAndValue.remove(property);
		cacheMapOfPropertyNameAndValue.put(property, value);
		//Remove the caching lifetime after receiving new data
		localCachingLifeTimeOfMap.remove(property);
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
					case NATURAL_MODE:
						String natural = convertByteToValue(reply);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NATURAL_MODE, natural);
						return natural;
					case TILE_ID:
						String tileID = convertByteToValue(reply);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TILE_MODE_ID, tileID);
						return tileID;
					case TILE_MODE_CONTROL:
						String tileModeControl = convertByteToValue(reply);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TILE_MODE_SETTINGS, tileModeControl);
						return tileModeControl;
					case NATURAL_SIZE:
						int naturalSize = Integer.parseInt(convertByteToValue(Arrays.copyOfRange(response, 9, 11)), 16);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NATURAL_SIZE, String.valueOf(naturalSize));
						return naturalSize;
					case BACKLIGHT:
						int backlight = Integer.parseInt(convertByteToValue(reply), 16);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.BACKLIGHT, String.valueOf(backlight));
						return backlight;
					case MUTE:
						int mute = Integer.parseInt(convertByteToValue(reply), 16);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.MUTE, String.valueOf(mute));
						return mute;
					case VOLUME:
						int volume = Integer.parseInt(convertByteToValue(reply), 16);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.VOLUME, String.valueOf(volume));
						return volume;
					case FAILOVER_INPUT_LIST:
						int len = response.length;
						reply = Arrays.copyOfRange(response, 7, len - 1);
						convertInputPriorityByValue(convertByteToValue(reply));
						return reply;
					case POWER_MANAGEMENT_MODE:
						reply = Arrays.copyOfRange(response, 9, 11);
						String powerManagement = convertByteToValue(reply);
						powerManagement = EnumTypeHandler.getNameEnumByValue(PowerManagementModeEnum.class, powerManagement);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.POWER_MANAGEMENT_MODE, powerManagement);
						return powerManagement;
					case POWER:
						for (Map.Entry<LgLCDConstants.powerStatusNames, byte[]> entry : LgLCDConstants.powerStatus.entrySet()) {
							if (Arrays.equals(reply, entry.getValue())) {
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.POWER, entry.getKey().toString());
								return entry.getKey();
							}
						}
						break;
					case NETWORK_SETTING:
						reply = Arrays.copyOfRange(response, 10, response.length - 1);
						convertNetworkSettingByValue(convertByteToValue(reply));
						return reply;
					case INPUT_SELECT:
					case INPUT:
						for (Map.Entry<LgLCDConstants.inputNames, byte[]> entry : LgLCDConstants.inputs.entrySet()) {
							if (Arrays.equals(reply, entry.getValue())) {
								String input = convertByteToValue(entry.getValue());
								String inputValue = EnumTypeHandler.getNameEnumByValue(FailOverInputSourceEnum.class, input);
								if (LgLCDConstants.NA.equalsIgnoreCase(inputValue)) {
									inputValue = EnumTypeHandler.getNameEnumByValue(InputSourceDropdown.class, input);
								}
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.INPUT_SELECT, inputValue);
								return entry.getKey();
							}
						}
						break;
					case TEMPERATURE:
						int temperature = Integer.parseInt(new String(reply), 16);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TEMPERATURE, String.valueOf(temperature));
						return temperature;
					case FAN_STATUS:
						for (Map.Entry<LgLCDConstants.fanStatusNames, byte[]> entry : LgLCDConstants.fanStatusCodes.entrySet()) {
							if (Arrays.equals(reply, entry.getValue())) {
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.FAN, entry.getKey().name());
								return entry.getKey();
							}
						}
						break;
					case SYNC_STATUS:
						reply = Arrays.copyOfRange(response, 7, 11);
						for (Map.Entry<LgLCDConstants.syncStatusNames, byte[]> entry : LgLCDConstants.syncStatusCodes.entrySet()) {
							if (Arrays.equals(reply, entry.getValue())) {
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.SIGNAL, entry.getKey().toString());
								return entry.getKey();
							}
						}
						break;
					case SERIAL_NUMBER:
						byte[] data = Arrays.copyOfRange(response, 7, 19);
						String serialNumber = convertByteToValue(data);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.SERIAL_NUMBER, serialNumber);
						return serialNumber;
					case FAILOVER:
						String failOver = convertByteToValue(reply);
						for (FailOverEnum name : FailOverEnum.values()) {
							if (name.getValue().equals(failOver)) {
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.FAILOVER_MODE, name.getName());
								return name.getName();
							}
						}
						break;
					case SOFTWARE_VERSION:
						data = Arrays.copyOfRange(response, 7, 13);
						String softwareVersion = convertByteToValue(data);
						//Custom software with format xx.xx.xx
						StringBuilder stringBuilder = new StringBuilder();
						for (int i = 0; i < softwareVersion.length(); i = i + 2) {
							stringBuilder.append(softwareVersion, i, i + 2);
							if (i != softwareVersion.length() - 2) {
								stringBuilder.append(LgLCDConstants.DOT);
							}
						}
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.SOFTWARE_VERSION, stringBuilder.toString());
						return stringBuilder.toString();
					case DISPLAY_STAND_BY_MODE:
						String pdm = convertByteToValue(reply);
						for (PowerManagement name : PowerManagement.values()) {
							if (name.getValue().equals(pdm)) {
								if (!localCacheMapOfPropertyNameAndValue.isEmpty()) {
									localCacheMapOfPropertyNameAndValue.remove(LgLCDConstants.DISPLAY_STAND_BY_MODE);
								}
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.DISPLAY_STAND_BY_MODE, name.getName());
								if (PowerManagement.OFF.getName().equals(name.getName())) {
									return name.getName();
								}
								return LgLCDConstants.ON;
							}
						}
						break;
					case DATE:
						data = Arrays.copyOfRange(response, 7, 13);
						String date = convertDateFormatByValue(data, false);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.DATE, date);
						return date;
					case TIME:
						data = Arrays.copyOfRange(response, 7, 13);
						String time = convertDateFormatByValue(data, true);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TIME, time);
						return time;
					case TILE_MODE_SETTINGS:
						byte[] typeModeStatus = Arrays.copyOfRange(response, 7, 9);
						byte[] typeModeColumn = Arrays.copyOfRange(response, 9, 11);
						byte[] typeModeRow = Arrays.copyOfRange(response, 11, 13);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TILE_MODE_COLUMN, String.valueOf(Integer.parseInt(convertByteToValue(typeModeColumn), 16)));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TILE_MODE_ROW, String.valueOf(Integer.parseInt(convertByteToValue(typeModeRow), 16)));
						String tileMode = convertByteToValue(typeModeStatus);
						for (TileMode name : TileMode.values()) {
							if (name.isStatus() && name.getValue().equals(tileMode)) {
								updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TILE_MODE, name.getName());
								return name.getName();
							}
						}
						break;
					case ASPECT_RATIO:
						String aspectRatio = EnumTypeHandler.getNameEnumByValue(AspectRatio.class, convertByteToValue(reply));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.ASPECT_RATIO, aspectRatio);
						return aspectRatio;
					case BRIGHTNESS_CONTROL:
						String brightness = EnumTypeHandler.getNameEnumByValue(BrightnessSize.class, convertByteToValue(reply));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.BRIGHTNESS_CONTROL, brightness);
						return brightness;
					case PICTURE_MODE:
						String pictureMode = EnumTypeHandler.getNameEnumByValue(PictureMode.class, convertByteToValue(reply));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.PICTURE_MODE, pictureMode);
						return pictureMode;
					case BRIGHTNESS:
						reply = Arrays.copyOfRange(response, 7, 9);
						String brightnessMode = String.valueOf(Integer.parseInt(convertByteToValue(reply), 16));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.BRIGHTNESS, brightnessMode);
						return brightnessMode;
					case CONTRAST:
						reply = Arrays.copyOfRange(response, 7, 9);
						String sharpness = String.valueOf(Integer.parseInt(convertByteToValue(reply), 16));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.CONTRAST, sharpness);
						return sharpness;
					case SHARPNESS:
						reply = Arrays.copyOfRange(response, 7, 9);
						String sharpnessValue = String.valueOf(Integer.parseInt(convertByteToValue(reply), 16));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.SHARPNESS, sharpnessValue);
						return sharpnessValue;
					case SCREEN_COLOR:
						reply = Arrays.copyOfRange(response, 7, 9);
						String tint = String.valueOf(Integer.parseInt(convertByteToValue(reply), 16));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.SCREEN_COLOR, tint);
						return tint;
					case TINT:
						reply = Arrays.copyOfRange(response, 7, 9);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.TINT, convertByteToValue(reply));
						return reply;
					case COLOR_TEMPERATURE:
						reply = Arrays.copyOfRange(response, 7, 9);
						String colorTemperature = String.valueOf(Integer.parseInt(convertByteToValue(reply), 16));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.COLOR_TEMPERATURE, colorTemperature);
						return colorTemperature;
					case BALANCE:
						reply = Arrays.copyOfRange(response, 7, 9);
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.BALANCE, convertByteToValue(reply));
						return reply;
					case SOUND_MODE:
						String soundModeValue = EnumTypeHandler.getNameEnumByValue(SoundMode.class, convertByteToValue(reply));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.SOUND_MODE, soundModeValue);
						return soundModeValue;
					case NO_SIGNAL_POWER_OFF:
						String noSignal = String.valueOf(Integer.parseInt(convertByteToValue(reply)));
						String noSignalValue = LgLCDConstants.ON;
						if (String.valueOf(LgLCDConstants.ZERO).equals(noSignal)) {
							noSignalValue = LgLCDConstants.OFF;
						}
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NO_SIGNAL_POWER_OFF, noSignalValue);
						return noSignalValue;
					case NO_IR_POWER_OFF:
						String noIRPower = String.valueOf(Integer.parseInt(convertByteToValue(reply)));
						String noIRPowerValue = LgLCDConstants.ON;
						if (String.valueOf(LgLCDConstants.ZERO).equals(noIRPower)) {
							noIRPowerValue = LgLCDConstants.OFF;
						}
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.NO_IR_POWER_OFF, noIRPowerValue);
						return noIRPowerValue;
					case LANGUAGE:
						String languageValue = EnumTypeHandler.getNameEnumByValue(Language.class, convertByteToValue(reply));
						if (!LgLCDConstants.NA.equals(languageValue)) {
							updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.LANGUAGE, languageValue);
						}
						return languageValue;
					case POWER_ON_STATUS:
						String powerOnStatus = EnumTypeHandler.getNameEnumByValue(PowerStatus.class, convertByteToValue(reply));
						updateCachedDeviceData(localCacheMapOfPropertyNameAndValue, LgLCDConstants.POWER_ON_STATUS, powerOnStatus);
						return powerOnStatus;
					case REBOOT:
						reply = Arrays.copyOfRange(response, 7, 9);
						String rebootValue = convertByteToValue(reply);
						if (!LgLCDConstants.REBOOT_VALUE.equals(rebootValue)) {
							throw new ResourceNotReachableException("NG reply");
						}
						return rebootValue;
					default:
						logger.debug("this command name is not supported" + expectedResponse);
				}
			} else if (Arrays.equals(responseStatus, LgLCDConstants.replyStatusCodes.get(replyStatusNames.NG))) {
				switch (expectedResponse) {
					case FAN_STATUS: {
						return LgLCDConstants.fanStatusNames.NOT_SUPPORTED;
					}
					default: {
						if (this.logger.isErrorEnabled()) {
							this.logger.error("error: NG reply: " + this.host + " port: " + this.getPort());
						}
						throw new ResourceNotReachableException("NG reply");
					}
				}
			}
		} else {
			if (this.logger.isErrorEnabled()) {
				this.logger.error("error: Unexpected reply: " + this.host + " port: " + this.getPort());
			}
			throw new RuntimeException("Error Unexpected reply");
		}

		return LgLCDConstants.NA;
	}

	/**
	 * Convert input priority by value
	 *
	 * @param inputPriority the inputPriority is String value
	 */
	private void convertInputPriorityByValue(String inputPriority) {
		int index = 1;
		cacheMapOfPriorityInputAndValue = new HashMap<>();
		for (int i = 0; i < inputPriority.length(); i = i + 2) {
			String value = inputPriority.substring(i, i + 2);
			cacheMapOfPriorityInputAndValue.put(LgLCDConstants.PRIORITY + index, EnumTypeHandler.getNameEnumByValue(FailOverInputSourceEnum.class, value));
			index++;
		}
	}

	/**
	 * Convert network setting by value
	 *
	 * @param networkResponse the networkResponse is String value
	 */
	private void convertNetworkSettingByValue(String networkResponse) {
		String[] networkArray = networkResponse.split(LgLCDConstants.SPACE);
		StringBuilder stringBuilder = new StringBuilder();
		// value of network settings will be 172000001001 255255255000 172000001001 172000000003
		try {
			convertNetworkSettingToValue(stringBuilder, networkArray[networkArray.length - 4]);
			localCacheMapOfPropertyNameAndValue.put(LgLCDConstants.IP_ADDRESS, stringBuilder.toString());

			stringBuilder = new StringBuilder();
			convertNetworkSettingToValue(stringBuilder, networkArray[networkArray.length - 3]);
			localCacheMapOfPropertyNameAndValue.put(LgLCDConstants.SUBNET_MASK, stringBuilder.toString());

			stringBuilder = new StringBuilder();
			convertNetworkSettingToValue(stringBuilder, networkArray[networkArray.length - 2]);
			localCacheMapOfPropertyNameAndValue.put(LgLCDConstants.GATEWAY, stringBuilder.toString());

			stringBuilder = new StringBuilder();
			convertNetworkSettingToValue(stringBuilder, networkArray[networkArray.length - 1]);
			localCacheMapOfPropertyNameAndValue.put(LgLCDConstants.DNS_SERVER, stringBuilder.toString());
		} catch (Exception e) {
			localCacheMapOfPropertyNameAndValue.put(LgLCDConstants.IP_ADDRESS, LgLCDConstants.NA);
			localCacheMapOfPropertyNameAndValue.put(LgLCDConstants.SUBNET_MASK, LgLCDConstants.NA);
			localCacheMapOfPropertyNameAndValue.put(LgLCDConstants.GATEWAY, LgLCDConstants.NA);
			localCacheMapOfPropertyNameAndValue.put(LgLCDConstants.DNS_SERVER, LgLCDConstants.NA);
		}
	}

	/**
	 * Convert data of network settings by value
	 *
	 * @param propertyName the propertyName is name of property
	 * @param networkValue the networkValue is value as String
	 */
	private void convertNetworkSettingToValue(StringBuilder propertyName, String networkValue) {
		//The network example value would be 192168000001, we will convert it to 192.168.0.1
		for (int i = 0; i < networkValue.length(); i = i + 3) {
			String value = networkValue.substring(i, i + 3);
			propertyName.append(Integer.parseInt(value));
			if (i != networkValue.length() - 3) {
				propertyName.append(LgLCDConstants.DOT);
			}
		}
	}

	/**
	 * Convert byte to value
	 *
	 * @param bytes is data represented as bytes
	 * @return String is data after converting byte to String
	 */
	private String convertByteToValue(byte[] bytes) {
		StringBuilder stringBuilder = new StringBuilder();
		for (byte byteValue : bytes) {
			stringBuilder.append((char) (byteValue));
		}
		return stringBuilder.toString();
	}

	/**
	 * Convert value to format month/day/year
	 *
	 * @param data the data is data of the response
	 * @param isTimeFormat the isTimeFormat is boolean value
	 * @return String is format of date
	 */
	private String convertDateFormatByValue(byte[] data, boolean isTimeFormat) {
		StringBuilder stringBuilder = new StringBuilder();
		StringBuilder dateValue = new StringBuilder();
		String year = LgLCDConstants.EMPTY_STRING;
		for (byte byteValue : data) {
			stringBuilder.append((char) (byteValue));
		}

		//The value example 173B00 with 17 is hours, 3B is minutes, and 00 is seconds
		//convert Hex to decimal data to 173B00 to 23:59:00
		String defaultTime = LgLCDConstants.AM;
		if (isTimeFormat) {
			for (int i = 0; i < stringBuilder.length() - 3; i = i + 2) {
				int hexValue = Integer.parseInt(stringBuilder.substring(i, i + 2), 16);
				if (i == 0) {
					if (hexValue == 0) {
						defaultTime = LgLCDConstants.PM;
						hexValue = 12;
					} else if (hexValue > 12) {
						defaultTime = LgLCDConstants.PM;
						hexValue = hexValue - 12;
					}
					dateValue.append(hexValue);
				} else {
					if (hexValue < 10) {
						dateValue.append(LgLCDConstants.COLON + LgLCDConstants.ZERO + hexValue);
					} else {
						dateValue.append(LgLCDConstants.COLON + hexValue);
					}
					dateValue.append(LgLCDConstants.SPACE + defaultTime);
				}
			}
			return dateValue.toString();
		}
		//The value example 0c011F with 0c is year, 01 is month, and 1F is day
		//convert Hex to decimal data to 0c011f to 1/31/2022
		//the year format = 2010 + 0c in(0c111F)
		for (int i = 0; i < stringBuilder.length() - 1; i = i + 2) {
			int hexValue = Integer.parseInt(stringBuilder.substring(i, i + 2), 16);
			if (i == 0) {
				year = String.valueOf(2010 + hexValue);
			} else {
				dateValue.append(hexValue + "/");
			}
		}
		return dateValue.append(year).toString();
	}

	/**
	 * Add switch is control property for metric
	 *
	 * @param stats list statistic
	 * @param name String name of metric
	 * @return AdvancedControllableProperty switch instance
	 */
	private AdvancedControllableProperty controlSwitch(Map<String, String> stats, String name, String value, String labelOff, String labelOn) {
		if (StringUtils.isNullOrEmpty(value) || LgLCDConstants.NA.equals(value)) {
			value = LgLCDConstants.NA;
			stats.put(name, value);
			// if response data is null or none. Only display monitoring data not display controlling data
			return null;
		}
		stats.put(name, value);
		return createSwitch(name, Integer.parseInt(value), labelOff, labelOn);
	}

	/**
	 * Create switch is control property for metric
	 *
	 * @param name the name of property
	 * @param status initial status (0|1)
	 * @return AdvancedControllableProperty switch instance
	 */
	private AdvancedControllableProperty createSwitch(String name, int status, String labelOff, String labelOn) {
		AdvancedControllableProperty.Switch toggle = new AdvancedControllableProperty.Switch();
		toggle.setLabelOff(labelOff);
		toggle.setLabelOn(labelOn);

		AdvancedControllableProperty advancedControllableProperty = new AdvancedControllableProperty();
		advancedControllableProperty.setName(name);
		advancedControllableProperty.setValue(status);
		advancedControllableProperty.setType(toggle);
		advancedControllableProperty.setTimestamp(new Date());
		return advancedControllableProperty;
	}

	/**
	 * Create control slider is control property for the metric
	 *
	 * @param name the name of the metric
	 * @param value the value of the metric
	 * @param rangeStart is the starting number of the range
	 * @param rangeEnd is the end number of the range
	 * @return AdvancedControllableProperty slider instance
	 */
	private AdvancedControllableProperty createSlider(String name, Float value, String rangeStart, String rangeEnd) {
		AdvancedControllableProperty.Slider slider = new AdvancedControllableProperty.Slider();
		slider.setLabelEnd(String.valueOf(rangeEnd));
		slider.setLabelStart(String.valueOf(rangeStart));
		slider.setRangeEnd(Float.valueOf(rangeEnd));
		slider.setRangeStart(Float.valueOf(rangeStart));

		return new AdvancedControllableProperty(name, new Date(), slider, value);
	}

	/**
	 * Create control slider is control property for the metric
	 *
	 * @param name name of the slider
	 * @param stats list of statistics
	 * @param rangeStart is the starting number of the range
	 * @param rangeEnd is the end number of the range
	 * @return AdvancedControllableProperty slider instance if add slider success else will is null
	 */
	private AdvancedControllableProperty createControlSlider(String name, String value, Map<String, String> stats, String rangeStart, String rangeEnd) {
		if (StringUtils.isNullOrEmpty(value) || LgLCDConstants.NA.equals(value)) {
			stats.put(name, LgLCDConstants.NA);
			return null;
		}
		stats.put(name, value);
		return createSlider(name, Float.valueOf(value), rangeStart, rangeEnd);
	}

	/**
	 * Add dropdown is control property for metric
	 *
	 * @param stats list statistic
	 * @param options list select
	 * @param name String name of metric
	 * @return AdvancedControllableProperty dropdown instance if add dropdown success else will is null
	 */
	private AdvancedControllableProperty controlDropdown(Map<String, String> stats, String[] options, String name, String value) {
		if (StringUtils.isNullOrEmpty(value) || LgLCDConstants.NA.equals(value)) {
			stats.put(name, LgLCDConstants.NA);
			return null;
		}
		stats.put(name, value);
		return createDropdown(name, options, value);
	}

	/***
	 * Create dropdown advanced controllable property
	 *
	 * @param name the name of the control
	 * @param initialValue initial value of the control
	 * @return AdvancedControllableProperty dropdown instance
	 */
	private AdvancedControllableProperty createDropdown(String name, String[] values, String initialValue) {
		AdvancedControllableProperty.DropDown dropDown = new AdvancedControllableProperty.DropDown();
		dropDown.setOptions(values);
		dropDown.setLabels(values);

		return new AdvancedControllableProperty(name, new Date(), dropDown, initialValue);
	}

	/**
	 * Create a button.
	 *
	 * @param name name of the button
	 * @param label label of the button
	 * @param labelPressed label of the button after pressing it
	 * @param gracePeriod grace period of button
	 * @return This returns the instance of {@link AdvancedControllableProperty} type Button.
	 */
	private AdvancedControllableProperty createButton(String name, String label, String labelPressed, long gracePeriod) {
		AdvancedControllableProperty.Button button = new AdvancedControllableProperty.Button();
		button.setLabel(label);
		button.setLabelPressed(labelPressed);
		button.setGracePeriod(gracePeriod);
		return new AdvancedControllableProperty(name, new Date(), button, LgLCDConstants.NA);
	}

	/**
	 * This method is used to validate input config management from user
	 */
	private void convertConfigManagement() {
		isConfigManagement = StringUtils.isNotNullOrEmpty(this.configManagement) && this.configManagement.equalsIgnoreCase(LgLCDConstants.IS_VALID_CONFIG_MANAGEMENT);
	}

	/**
	 * This method is used to convert or validate the user input
	 */
	private void convertCacheLifetime() {
		try {
			currentCachingLifetime = Integer.parseInt(this.cachingLifetime);
			if (currentCachingLifetime <= LgLCDConstants.ZERO) {
				currentCachingLifetime = LgLCDConstants.DEFAULT_CACHING_LIFETIME;
			}
		} catch (Exception e) {
			currentCachingLifetime = LgLCDConstants.DEFAULT_CACHING_LIFETIME;
		}
	}

	/**
	 * This method is used to validate input delay time from user
	 */
	private void convertDelayTime() {
		try {
			commandsCoolDownDelay = Integer.parseInt(this.coolDownDelay);
			if (LgLCDConstants.MIN_DELAY_TIME >= commandsCoolDownDelay) {
				commandsCoolDownDelay = LgLCDConstants.MIN_DELAY_TIME;
			}
			if (LgLCDConstants.MAX_DELAY_TIME <= commandsCoolDownDelay) {
				commandsCoolDownDelay = LgLCDConstants.MAX_DELAY_TIME;
			}
		} catch (Exception e) {
			commandsCoolDownDelay = LgLCDConstants.DEFAULT_DELAY_TIME;
		}
	}

	/**
	 * This method is used to validate input config timeout from user
	 */
	private void convertConfigTimeout() {
		int configTimeout;
		try {
			configTimeout = Integer.parseInt(this.configTimeout);
			if (LgLCDConstants.DEFAULT_CONFIG_TIMEOUT >= configTimeout) {
				configTimeout = LgLCDConstants.DEFAULT_CONFIG_TIMEOUT;
			}
			if (LgLCDConstants.MAX_CONFIG_TIMEOUT <= configTimeout) {
				configTimeout = LgLCDConstants.MAX_CONFIG_TIMEOUT;
			}
		} catch (Exception e) {
			configTimeout = LgLCDConstants.DEFAULT_CONFIG_TIMEOUT;
		}
		defaultConfigTimeout = configTimeout / 100;
	}

	/**
	 * This method is used to validate input config timeout from user
	 */
	private void convertPollingInterval() {
		int pollingIntervalValue;
		try {
			pollingIntervalValue = Integer.parseInt(this.pollingInterval);
			if (pollingIntervalValue < LgLCDConstants.DEFAULT_POLLING_INTERVAL) {
				pollingIntervalValue = LgLCDConstants.DEFAULT_POLLING_INTERVAL;
			}
		} catch (Exception e) {
			pollingIntervalValue = LgLCDConstants.DEFAULT_POLLING_INTERVAL;
		}
		pollingIntervalInIntValue = pollingIntervalValue;
	}
}