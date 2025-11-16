/*
 * ====================================================================================
 * COMPLEX JAVA EVENT-DRIVEN SIMULATION (SMART CITY) - V2 (1100+ Lines)
 * ====================================================================================
 *
 * This single file simulates a complex, event-driven backend system.
 * It is designed to be highly decoupled and asynchronous, with a completely
 * different dependency structure than a typical layered CRUD application.
 *
 * ARCHITECTURE:
 * 1. EventBus: A central, thread-safe, asynchronous message broker.
 * 2. Events: A hierarchy of data classes representing messages.
 * 3. Publishers: (e.g., IoT Sensors, Simulators) - Run on separate threads,
 * generate events, and publish them to the EventBus.
 * 4. Subscribers: (e.g., Services) - Register with the EventBus to listen for
 * specific events. They process data and can publish new events, but
 * they have no direct reference to each other.
 *
 * DEPENDENCY GRAPH (Conceptual):
 * [SensorSimulator] -------> [EventBus] <------ [DataAnalyticsService]
 * [TrafficSimulator] ------> [EventBus] <------ [AnomalyDetectionService]
 * [SmartMeterSimulator] ---> [EventBus] <------ [PowerManagementService]
 * [EmergencySimulator] ----> [EventBus] <------ [EmergencyDispatchService]
 *
 * [AnomalyDetectionService] -> [EventBus] <------ [ControlCenterService]
 * [ControlCenterService] ----> [EventBus] <------ [PublicDisplayService]
 * [ControlCenterService] ----> [EventBus] <------ [EmergencyDispatchService]
 *
 * All components are decoupled via the EventBus.
 * ====================================================================================
 */

// --- Imports (Grouped for clarity) ---
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * =======================================================================
 * PACKAGE: com.smartcity.core
 * DESCRIPTION: Core components of the event-driven framework.
 * =======================================================================
 */

/**
 * ---------------------------------
 * FILE: com/smartcity/core/Event.java
 * ---------------------------------
 * Abstract base class for all events in the system.
 * All events are immutable.
 */
abstract class Event {
    private final UUID eventId;
    private final Instant timestamp;

    /**
     * Constructs a new Event, capturing the current time and a unique ID.
     */
    public Event() {
        this.eventId = UUID.randomUUID();
        this.timestamp = Instant.now();
    }

    /**
     * Gets the unique ID of the event.
     * @return The UUID of the event.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Gets the time the event was created.
     * @return The Instant the event was created.
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * Generates a string representation of the base event.
     * @return A string containing event class name and timestamp.
     */
    @Override
    public String toString() {
        return String.format("[%s @ %s]",
            this.getClass().getSimpleName(),
            this.timestamp
        );
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/core/Subscriber.java
 * ---------------------------------
 * A functional interface for any class that wishes to subscribe to events.
 */
@FunctionalInterface
interface Subscriber {
    /**
     * Called by the EventBus when a subscribed event is published.
     * This method will be executed on a dispatcher thread.
     *
     * @param event The event that was published.
     */
    void onEvent(Event event);
}

/**
 * ---------------------------------
 * FILE: com/smartcity/core/EventBus.java
 * ---------------------------------
 * The central, thread-safe, asynchronous event broker.
 * This is the only class that most components "depend" on.
 */
class EventBus {
    // A map where the key is the Event class (e.g., IotSensorReadingEvent.class)
    // and the value is a thread-safe list of subscribers.
    private final Map<Class<? extends Event>, List<Subscriber>> subscribers;
    private final ExecutorService executor;
    private final SimpleLogger logger = new SimpleLogger("EventBus");

    /**
     * Constructs a new EventBus with a fixed-size thread pool
     * for asynchronous event dispatching.
     */
    public EventBus() {
        // Use thread-safe collections for a concurrent environment
        this.subscribers = new ConcurrentHashMap<>();
        // Use a virtual thread-per-task executor for high throughput I/O-bound tasks (subscribers)
        // If virtual threads aren't available, a cached or fixed pool is also fine.
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        // this.executor = Executors.newFixedThreadPool(10); // Fallback
        logger.log("EventBus initialized with VirtualThreadPerTaskExecutor.");
    }

    /**
     * Registers a subscriber for a specific type of event.
     * A subscriber can be registered for multiple event types by calling this
     * method multiple times.
     *
     * @param eventType The class of the event to listen for (e.g., IotSensorReadingEvent.class)
     * @param subscriber The subscriber instance that will receive the event.
     */
    public <T extends Event> void register(Class<T> eventType, Subscriber subscriber) {
        // computeIfAbsent ensures thread-safe creation of the list
        // CopyOnWriteArrayList is used for thread-safe iteration while allowing concurrent modification
        List<Subscriber> subs = subscribers.computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>());
        subs.add(subscriber);
        logger.log("Registered " + subscriber.getClass().getSimpleName() + " for " + eventType.getSimpleName());
    }

    /**
     * Publishes an event to all registered subscribers.
     * The dispatching of events to subscribers is done asynchronously
     * on a separate thread pool.
     *
     * @param event The event to publish.
     */
    public void publish(Event event) {
        if (event == null) {
            logger.warn("Received null event. Ignoring.");
            return;
        }

        // Get subscribers for this specific event type
        List<Subscriber> specificSubs = subscribers.get(event.getClass());
        if (specificSubs != null) {
            dispatchToSubscribers(specificSubs, event);
        }

        // Get subscribers registered for "all" events (using the base Event.class)
        List<Subscriber> allEventSubs = subscribers.get(Event.class);
        if (allEventSubs != null) {
            dispatchToSubscribers(allEventSubs, event);
        }
    }

    /**
     * Helper method to submit event-handling tasks to the thread pool.
     * @param subs List of subscribers to notify.
     * @param event The event to dispatch.
     */
    private void dispatchToSubscribers(List<Subscriber> subs, Event event) {
        for (final Subscriber subscriber : subs) {
            executor.submit(() -> {
                try {
                    // This is where the subscriber's logic is actually executed
                    subscriber.onEvent(event);
                } catch (Exception e) {
                    logger.error("Subscriber " + subscriber.getClass().getSimpleName() +
                        " threw an exception processing " + event.getClass().getSimpleName() +
                        ": " + e.getMessage(), e);
                }
            });
        }
    }

    /**
     * Shuts down the event bus and its thread pool.
     */
    public void shutdown() {
        logger.log("Shutting down event dispatcher pool...");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Dispatcher pool did not shut down gracefully. Forcing...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for pool shutdown.", e);
            executor.shutdownNow();
        }
        logger.log("EventBus shut down.");
    }
}

/**
 * =======================================================================
 * PACKAGE: com.smartcity.events
 * DESCRIPTION: Data contracts (POCOs) for all events.
 * =======================================================================
 */

/**
 * ---------------------------------
 * FILE: com/smartcity/events/IotSensorReadingEvent.java
 * ---------------------------------
 */
final class IotSensorReadingEvent extends Event {
    private final String sensorId;
    private final double temperature; // Celsius
    private final double humidity;    // %
    private final double pressure;    // hPa

    public IotSensorReadingEvent(String sensorId, double temperature, double humidity, double pressure) {
        super();
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.humidity = humidity;
        this.pressure = pressure;
    }

    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public double getHumidity() { return humidity; }
    public double getPressure() { return pressure; }

    @Override
    public String toString() {
        return String.format("%s [Sensor: %s, Temp: %.1fC, Hum: %.1f%%]",
            super.toString(), sensorId, temperature, humidity);
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/events/TrafficDataEvent.java
 * ---------------------------------
 */
final class TrafficDataEvent extends Event {
    private final String cameraId;
    private final int vehicleCount;
    private final double averageSpeed; // km/h

    public TrafficDataEvent(String cameraId, int vehicleCount, double averageSpeed) {
        super();
        this.cameraId = cameraId;
        this.vehicleCount = vehicleCount;
        this.averageSpeed = averageSpeed;
    }

    public String getCameraId() { return cameraId; }
    public int getVehicleCount() { return vehicleCount; }
    public double getAverageSpeed() { return averageSpeed; }

    @Override
    public String toString() {
        return String.format("%s [Camera: %s, Vehicles: %d, AvgSpeed: %.1f km/h]",
            super.toString(), cameraId, vehicleCount, averageSpeed);
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/events/PowerUsageEvent.java
 * ---------------------------------
 * NEW EVENT
 */
final class PowerUsageEvent extends Event {
    private final String meterId;
    private final double kilowattHours; // Total kWh consumed
    private final double currentDrawAmps; // Instantaneous draw

    public PowerUsageEvent(String meterId, double kilowattHours, double currentDrawAmps) {
        super();
        this.meterId = meterId;
        this.kilowattHours = kilowattHours;
        this.currentDrawAmps = currentDrawAmps;
    }

    public String getMeterId() { return meterId; }
    public double getKilowattHours() { return kilowattHours; }
    public double getCurrentDrawAmps() { return currentDrawAmps; }

    @Override
    public String toString() {
        return String.format("%s [Meter: %s, Draw: %.2fA, Total: %.2fkWh]",
            super.toString(), meterId, currentDrawAmps, kilowattHours);
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/events/EmergencyServiceEvent.java
 * ---------------------------------
 * NEW EVENT
 */
final class EmergencyServiceEvent extends Event {
    public enum EmergencyType { POLICE, FIRE, MEDICAL }
    private final EmergencyType type;
    private final String location;
    private final String description;
    private final int severity; // 1-5

    public EmergencyServiceEvent(EmergencyType type, String location, String description, int severity) {
        super();
        this.type = type;
        this.location = location;
        this.description = description;
        this.severity = severity;
    }

    public EmergencyType getType() { return type; }
    public String getLocation() { return location; }
    public String getDescription() { return description; }
    public int getSeverity() { return severity; }

    @Override
    public String toString() {
        return String.format("%s [Type: %s, Loc: %s, Sev: %d, Desc: %s]",
            super.toString(), type, location, severity, description);
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/events/AnomalyDetectedEvent.java
 * ---------------------------------
 * An event published BY a service, not a sensor.
 */
final class AnomalyDetectedEvent extends Event {
    public enum Severity { INFO, WARNING, CRITICAL }
    public enum AnomalyType { HIGH_TEMP, GRIDLOCK, POWER_SURGE, SYSTEM_FAILURE, UNKNOWN }

    private final AnomalyType anomalyType;
    private final String reason;
    private final Event triggeringEvent;
    private final Severity severity;

    public AnomalyDetectedEvent(AnomalyType type, String reason, Event triggeringEvent, Severity severity) {
        super();
        this.anomalyType = type;
        this.reason = reason;
        this.triggeringEvent = triggeringEvent;
        this.severity = severity;
    }

    public AnomalyType getAnomalyType() { return anomalyType; }
    public String getReason() { return reason; }
    public Event getTriggeringEvent() { return triggeringEvent; }
    public Severity getSeverity() { return severity; }

    @Override
    public String toString() {
        return String.format("%s [%s] [%s] [Reason: %s] [Trigger: %s]",
            super.toString(), severity, anomalyType, reason, triggeringEvent.getClass().getSimpleName());
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/events/ActionDispatchEvent.java
 * ---------------------------------
 * An event that commands another service to perform an action.
 */
final class ActionDispatchEvent extends Event {
    private final String actionType; // e.g., "ACTIVATE_COOLING", "REDIRECT_TRAFFIC", "DISPATCH_UNIT"
    private final String targetDevice;
    private final Map<String, String> parameters;

    public ActionDispatchEvent(String actionType, String targetDevice, Map<String, String> parameters) {
        super();
        this.actionType = actionType;
        this.targetDevice = targetDevice;
        this.parameters = parameters;
    }

    public String getActionType() { return actionType; }
    public String getTargetDevice() { return targetDevice; }
    public Map<String, String> getParameters() { return parameters; }

    @Override
    public String toString() {
        return String.format("%s [Action: %s, Target: %s, Params: %s]",
            super.toString(), actionType, targetDevice, parameters);
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/events/PublicAlertEvent.java
 * ---------------------------------
 * NEW EVENT - An event to be consumed by public-facing displays.
 */
final class PublicAlertEvent extends Event {
    private final String message;
    private final int durationSeconds;
    private final String targetArea; // e.g., "ALL", "DISTRICT-A"

    public PublicAlertEvent(String message, int durationSeconds, String targetArea) {
        super();
        this.message = message;
        this.durationSeconds = durationSeconds;
        this.targetArea = targetArea;
    }

    public String getMessage() { return message; }
    public int getDurationSeconds() { return durationSeconds; }
    public String getTargetArea() { return targetArea; }

    @Override
    public String toString() {
        return String.format("%s [Area: %s, Duration: %ds, Msg: %s]",
            super.toString(), targetArea, durationSeconds, message);
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/events/SystemHealthEvent.java
 * ---------------------------------
 */
final class SystemHealthEvent extends Event {
    private final String serviceName;
    private final String status; // e.g., "OK", "DEGRADED"
    private final long uptimeMillis;

    public SystemHealthEvent(String serviceName, String status, long uptimeMillis) {
        super();
        this.serviceName = serviceName;
        this.status = status;
        this.uptimeMillis = uptimeMillis;
    }

    public String getServiceName() { return serviceName; }
    public String getStatus() { return status; }
    public long getUptimeMillis() { return uptimeMillis; }

    @Override
    public String toString() {
        return String.format("%s [Service: %s, Status: %s, Uptime: %ds]",
            super.toString(), serviceName, status, uptimeMillis / 1000);
    }
}


/**
 * =======================================================================
 * PACKAGE: com.smartcity.sensors (Publishers)
 * DESCRIPTION: Simulators for edge devices that publish events.
 * =======================================================================
 */

/**
 * ---------------------------------
 * FILE: com/smartcity/sensors/IotSensorSimulator.java
 * ---------------------------------
 * A runnable class that simulates a weather sensor.
 */
class IotSensorSimulator implements Runnable {
    private final EventBus eventBus;
    private final String sensorId;
    private final Random random;
    private final SimpleLogger logger;
    private volatile boolean running = true;

    public IotSensorSimulator(EventBus eventBus, String sensorId) {
        this.eventBus = eventBus;
        this.sensorId = sensorId;
        this.random = new Random();
        this.logger = new SimpleLogger("Sensor-" + sensorId);
    }

    public void stop() {
        this.running = false;
    }

    @Override
    public void run() {
        logger.log("Sensor simulation started.");
        while (running) {
            try {
                // Generate slightly fluctuating data
                double temp = 20.0 + (random.nextDouble() * 5.0) + (Math.sin(System.currentTimeMillis() / 10000.0) * 10);
                // Add a random spike
                if (random.nextDouble() < 0.05) {
                    temp += 25.0; // Sudden heat spike
                    logger.warn("!!! Generating anomalous temperature spike !!!");
                }
                double humidity = 50.0 + (random.nextDouble() * 10.0);
                double pressure = 1012.0 + (random.nextDouble() * 2.0);

                // Create and publish the event
                IotSensorReadingEvent event = new IotSensorReadingEvent(sensorId, temp, humidity, pressure);
                eventBus.publish(event);

                // Sleep for a random interval
                Thread.sleep(1000 + random.nextInt(1500));
            } catch (InterruptedException e) {
                logger.log("Sensor was interrupted. Shutting down.");
                this.running = false;
            }
        }
        logger.log("Sensor simulation stopped.");
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/sensors/TrafficCameraSimulator.java
 * ---------------------------------
 * A runnable class that simulates a traffic camera.
 */
class TrafficCameraSimulator implements Runnable {
    private final EventBus eventBus;
    private final String cameraId;
    private final Random random;
    private final SimpleLogger logger;
    private volatile boolean running = true;

    public TrafficCameraSimulator(EventBus eventBus, String cameraId) {
        this.eventBus = eventBus;
        this.cameraId = cameraId;
        this.random = new Random();
        this.logger = new SimpleLogger("Camera-" + cameraId);
    }

    public void stop() {
        this.running = false;
    }

    @Override
    public void run() {
        logger.log("Traffic camera simulation started.");
        while (running) {
            try {
                // Simulate traffic flow
                int vehicleCount = 50 + random.nextInt(100);
                double avgSpeed = 45.0 - (vehicleCount / 10.0) + (random.nextDouble() * 5.0);
                
                // Simulate a gridlock event
                if (random.nextDouble() < 0.1) {
                    vehicleCount = 200 + random.nextInt(50);
                    avgSpeed = 1.0 + random.nextDouble() * 2.0;
                    logger.warn("!!! Generating anomalous gridlock event !!!");
                }

                TrafficDataEvent event = new TrafficDataEvent(cameraId, vehicleCount, avgSpeed);
                eventBus.publish(event);

                // Sleep for 5 seconds
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                logger.log("Camera was interrupted. Shutting down.");
                this.running = false;
            }
        }
        logger.log("Camera simulation stopped.");
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/sensors/SmartMeterSimulator.java
 * ---------------------------------
 * NEW SIMULATOR - A runnable class that simulates a building's smart meter.
 */
class SmartMeterSimulator implements Runnable {
    private final EventBus eventBus;
    private final String meterId;
    private final Random random;
    private final SimpleLogger logger;
    private volatile boolean running = true;
    private double totalKWh = 1000.0; // Starting baseline

    public SmartMeterSimulator(EventBus eventBus, String meterId) {
        this.eventBus = eventBus;
        this.meterId = meterId;
        this.random = new Random();
        this.logger = new SimpleLogger("Meter-" + meterId);
    }

    public void stop() {
        this.running = false;
    }

    @Override
    public void run() {
        logger.log("Smart meter simulation started.");
        while (running) {
            try {
                // Simulate power draw
                double draw = 15.0 + (random.nextDouble() * 5.0) + (Math.sin(System.currentTimeMillis() / 5000.0) * 10);
                
                // Simulate a power surge
                if (random.nextDouble() < 0.05) {
                    draw = 100.0 + random.nextDouble() * 20.0; // Sudden surge
                    logger.warn("!!! Generating anomalous power surge !!!");
                }
                
                totalKWh += (draw * 2.0 / 60.0); // Assuming 2 second reading interval

                PowerUsageEvent event = new PowerUsageEvent(meterId, totalKWh, draw);
                eventBus.publish(event);

                Thread.sleep(2000); // Report every 2 seconds
            } catch (InterruptedException e) {
                logger.log("Meter was interrupted. Shutting down.");
                this.running = false;
            }
        }
        logger.log("Meter simulation stopped.");
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/sensors/EmergencyCallSimulator.java
 * ---------------------------------
 * NEW SIMULATOR - A runnable that simulates random 911 calls.
 */
class EmergencyCallSimulator implements Runnable {
    private final EventBus eventBus;
    private final Random random;
    private final SimpleLogger logger;
    private volatile boolean running = true;
    private final String[] locations = {"Main St & 2nd Ave", "City Hall", "District-B Park", "Highway 101 Exit 4"};
    private final String[] fireDesc = {"Structure fire", "Report of smoke", "Car fire"};
    private final String[] medicalDesc = {"Unconscious person", "Chest pains", "Traffic accident"};
    private final String[] policeDesc = {"Disturbance", "Theft reported", "Suspicious person"};

    public EmergencyCallSimulator(EventBus eventBus) {
        this.eventBus = eventBus;
        this.random = new Random();
        this.logger = new SimpleLogger("911-Dispatch");
    }

    public void stop() {
        this.running = false;
    }

    @Override
    public void run() {
        logger.log("Emergency call simulator started.");
        while (running) {
            try {
                // Wait a long random time between calls
                Thread.sleep(15000 + random.nextInt(30000));
                
                EmergencyServiceEvent.EmergencyType type = EmergencyServiceEvent.EmergencyType.values()[random.nextInt(3)];
                String location = locations[random.nextInt(locations.length)];
                String description = "";
                switch (type) {
                    case FIRE: description = fireDesc[random.nextInt(fireDesc.length)]; break;
                    case MEDICAL: description = medicalDesc[random.nextInt(medicalDesc.length)]; break;
                    case POLICE: description = policeDesc[random.nextInt(policeDesc.length)]; break;
                }
                int severity = 2 + random.nextInt(4); // 2-5
                
                EmergencyServiceEvent event = new EmergencyServiceEvent(type, location, description, severity);
                logger.warn("!!! SIMULATING INCOMING 911 CALL: " + event.toString() + " !!!");
                eventBus.publish(event);

            } catch (InterruptedException e) {
                logger.log("911 simulator was interrupted. Shutting down.");
                this.running = false;
            }
        }
        logger.log("Emergency call simulator stopped.");
    }
}


/**
 * =======================================================================
 * PACKAGE: com.smartcity.services (Subscribers)
 * DESCRIPTION: Services that react to events.
 * =======================================================================
 */

/**
 * ---------------------------------
 * FILE: com/smartcity/services/LoggingService.java
 * ---------------------------------
 * A simple subscriber that logs every single event.
 */
class LoggingService implements Subscriber {
    private final SimpleLogger logger = new SimpleLogger("LoggingService");

    /**
     * Registers this service with the event bus.
     * @param bus The EventBus instance.
     */
    public void register(EventBus bus) {
        bus.register(Event.class, this); // Subscribes to the base Event, gets all events
    }

    /**
     * Handles the event by logging its string representation.
     * @param event The event to log.
     */
    @Override
    public void onEvent(Event event) {
        // Just log the event's string representation
        logger.log("EVENT: " + event.toString());
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/services/DataAnalyticsService.java
 * ---------------------------------
 * A stateful service that performs simple analytics and reports periodically.
 * It is both a Subscriber (to data) and a Runnable (to report).
 */
class DataAnalyticsService implements Subscriber, Runnable {
    private final SimpleLogger logger = new SimpleLogger("AnalyticsService");
    private final ScheduledExecutorService reporter;

    // Stateful data (all concurrent-safe)
    private final AtomicLong sensorReadings = new AtomicLong(0);
    private final AtomicLong trafficReadings = new AtomicLong(0);
    private final AtomicLong powerReadings = new AtomicLong(0);
    private final ConcurrentHashMap<String, Double> lastTemps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> lastVehicleCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double> lastPowerDraw = new ConcurrentHashMap<>();

    /**
     * Constructs the service and its private, scheduled reporting thread.
     */
    public DataAnalyticsService() {
        this.reporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "analytics-reporter");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Registers this service with the event bus for all data events.
     * @param bus The EventBus instance.
     */
    public void register(EventBus bus) {
        bus.register(IotSensorReadingEvent.class, this);
        bus.register(TrafficDataEvent.class, this);
        bus.register(PowerUsageEvent.class, this);
    }

    /**
     * Starts the periodic reporting task.
     */
    public void startReporting() {
        // This is now the "run" method, executed by the ScheduledExecutor
        reporter.scheduleAtFixedRate(this, 10, 10, TimeUnit.SECONDS);
        logger.log("Periodic analytics reporting started.");
    }

    /**
     * Stops the periodic reporting task.
     */
    public void stopReporting() {
        reporter.shutdown();
    }

    /**
     * Handles incoming data events from the EventBus.
     * This method is called by the EventBus dispatcher threads.
     * @param event The data event.
     */
    @Override
    public void onEvent(Event event) {
        try {
            if (event instanceof IotSensorReadingEvent e) {
                sensorReadings.incrementAndGet();
                lastTemps.put(e.getSensorId(), e.getTemperature());
            } else if (event instanceof TrafficDataEvent e) {
                trafficReadings.incrementAndGet();
                lastVehicleCounts.put(e.getCameraId(), e.getVehicleCount());
            } else if (event instanceof PowerUsageEvent e) {
                powerReadings.incrementAndGet();
                lastPowerDraw.put(e.getMeterId(), e.getCurrentDrawAmps());
            }
        } catch (Exception e) {
            logger.error("Error processing analytics event: " + e.getMessage(), e);
        }
    }

    /**
     * This is the Runnable's run method, executed by the reporter's single thread.
     * It generates a summary report of all data collected.
     */
    @Override
    public void run() {
        logger.log("--- ANALYTICS REPORT ---");
        logger.log("Total Sensor Readings: " + sensorReadings.get());
        logger.log("Total Traffic Readings: " + trafficReadings.get());
        logger.log("Total Power Readings:   " + powerReadings.get());

        lastTemps.forEach((id, temp) ->
            logger.log(String.format("  -> Last Temp [%s]: %.1fC", id, temp))
        );
        lastVehicleCounts.forEach((id, count) ->
            logger.log(String.format("  -> Last Traffic [%s]: %d vehicles", id, count))
        );
        lastPowerDraw.forEach((id, draw) ->
            logger.log(String.format("  -> Last Power [%s]: %.2f Amps", id, draw))
        );
        logger.log("--- END REPORT ---");
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/services/AnomalyDetectionService.java
 * ---------------------------------
 * A service that subscribes to data and PUBLISHES a new event
 * when an anomaly is found.
 */
class AnomalyDetectionService implements Subscriber {
    private final SimpleLogger logger = new SimpleLogger("AnomalyService");
    private final EventBus eventBus;

    // Thresholds
    private final double temperatureThreshold = 40.0; // Celsius
    private final double gridlockSpeedThreshold = 5.0; // km/h
    private final int gridlockVehicleThreshold = 200; // vehicles
    private final double powerSurgeThreshold = 80.0; // Amps

    /**
     * Constructs the service.
     * @param eventBus Needs a reference to the bus to publish new events.
     */
    public AnomalyDetectionService(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    /**
     * Registers this service with the event bus.
     * @param bus The EventBus instance.
     */
    public void register(EventBus bus) {
        bus.register(IotSensorReadingEvent.class, this);
        bus.register(TrafficDataEvent.class, this);
        bus.register(PowerUsageEvent.class, this);
    }

    /**
     * Handles incoming data events and checks them against thresholds.
     * @param event The data event.
     */
    @Override
    public void onEvent(Event event) {
        try {
            if (event instanceof IotSensorReadingEvent e) {
                if (e.getTemperature() > temperatureThreshold) {
                    processHighTempAnomaly(e);
                }
            } else if (event instanceof TrafficDataEvent e) {
                if (e.getAverageSpeed() < gridlockSpeedThreshold && e.getVehicleCount() > gridlockVehicleThreshold) {
                    processGridlockAnomaly(e);
                }
            } else if (event instanceof PowerUsageEvent e) {
                if (e.getCurrentDrawAmps() > powerSurgeThreshold) {
                    processPowerSurgeAnomaly(e);
                }
            }
        } catch (Exception e) {
            logger.error("Error detecting anomaly: " + e.getMessage(), e);
        }
    }

    private void processHighTempAnomaly(IotSensorReadingEvent event) {
        String reason = String.format("HIGH_TEMP (%.1fC) detected at %s",
            event.getTemperature(), event.getSensorId());
        logger.warn("ANOMALY DETECTED: " + reason);

        // Publish a NEW event for other services to react to
        AnomalyDetectedEvent anomalyEvent = new AnomalyDetectedEvent(
            AnomalyDetectedEvent.AnomalyType.HIGH_TEMP,
            reason,
            event,
            AnomalyDetectedEvent.Severity.CRITICAL
        );
        this.eventBus.publish(anomalyEvent);
    }

    private void processGridlockAnomaly(TrafficDataEvent event) {
        String reason = String.format("GRIDLOCK (%.1f km/h, %d vehicles) detected at %s",
            event.getAverageSpeed(), event.getVehicleCount(), event.getCameraId());
        logger.warn("ANOMALY DETECTED: " + reason);

        AnomalyDetectedEvent anomalyEvent = new AnomalyDetectedEvent(
            AnomalyDetectedEvent.AnomalyType.GRIDLOCK,
            reason,
            event,
            AnomalyDetectedEvent.Severity.WARNING
        );
        this.eventBus.publish(anomalyEvent);
    }
    
    private void processPowerSurgeAnomaly(PowerUsageEvent event) {
        String reason = String.format("POWER_SURGE (%.2fA) detected at %s",
            event.getCurrentDrawAmps(), event.getMeterId());
        logger.warn("ANOMALY DETECTED: " + reason);

        AnomalyDetectedEvent anomalyEvent = new AnomalyDetectedEvent(
            AnomalyDetectedEvent.AnomalyType.POWER_SURGE,
            reason,
            event,
            AnomalyDetectedEvent.Severity.CRITICAL
        );
        this.eventBus.publish(anomalyEvent);
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/services/ControlCenterService.java
 * ---------------------------------
 * A service that subscribes to anomalies and dispatches actions.
 * This is a central "decision-making" service, but is still
 * decoupled from the services it's commanding.
 */
class ControlCenterService implements Subscriber {
    private final SimpleLogger logger = new SimpleLogger("ControlCenter");
    private final EventBus eventBus;

    public ControlCenterService(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    /**
     * Registers this service with the event bus.
     * @param bus The EventBus instance.
     */
    public void register(EventBus bus) {
        bus.register(AnomalyDetectedEvent.class, this);
    }

    /**
     * Handles anomaly events and decides what actions to take.
     * @param event The anomaly event.
     */
    @Override
    public void onEvent(Event event) {
        if (event instanceof AnomalyDetectedEvent e) {
            switch (e.getAnomalyType()) {
                case HIGH_TEMP:
                    dispatchCoolingAction(e);
                    break;
                case GRIDLOCK:
                    dispatchTrafficAction(e);
                    publishPublicAlert(e);
                    break;
                case POWER_SURGE:
                    dispatchPowerAction(e);
                    break;
                default:
                    logger.warn("No action defined for anomaly type: " + e.getAnomalyType());
            }
        }
    }

    private void dispatchCoolingAction(AnomalyDetectedEvent anomaly) {
        if (anomaly.getTriggeringEvent() instanceof IotSensorReadingEvent sensorEvent) {
            String target = "COOLING_UNIT_FOR_AREA_" + sensorEvent.getSensorId();
            Map<String, String> params = Map.of(
                "sensorId", sensorEvent.getSensorId(),
                "temperature", String.format("%.1f", sensorEvent.getTemperature())
            );
            ActionDispatchEvent action = new ActionDispatchEvent("ACTIVATE_COOLING", target, params);
            logger.log("Dispatching Action: " + action.toString());
            this.eventBus.publish(action);
        }
    }

    private void dispatchTrafficAction(AnomalyDetectedEvent anomaly) {
        if (anomaly.getTriggeringEvent() instanceof TrafficDataEvent trafficEvent) {
            String target = "TRAFFIC_LIGHT_GRID_" + trafficEvent.getCameraId();
            Map<String, String> params = Map.of(
                "cameraId", trafficEvent.getCameraId(),
                "avgSpeed", String.format("%.1f", trafficEvent.getAverageSpeed())
            );
            ActionDispatchEvent action = new ActionDispatchEvent("REROUTE_TRAFFIC_FLOW", target, params);
            logger.log("Dispatching Action: " + action.toString());
            this.eventBus.publish(action);
        }
    }
    
    private void dispatchPowerAction(AnomalyDetectedEvent anomaly) {
        if (anomaly.getTriggeringEvent() instanceof PowerUsageEvent powerEvent) {
            String target = "SUBSTATION_GRID_" + powerEvent.getMeterId();
            Map<String, String> params = Map.of(
                "meterId", powerEvent.getMeterId(),
                "draw", String.format("%.2f", powerEvent.getCurrentDrawAmps())
            );
            ActionDispatchEvent action = new ActionDispatchEvent("LOAD_BALANCE_GRID", target, params);
            logger.log("Dispatching Action: " + action.toString());
            this.eventBus.publish(action);
        }
    }

    private void publishPublicAlert(AnomalyDetectedEvent anomaly) {
        if (anomaly.getTriggeringEvent() instanceof TrafficDataEvent trafficEvent) {
            String message = String.format("MAJOR TRAFFIC GRIDLOCK near %s. Avoid area. Avg speed %.0f km/h.",
                trafficEvent.getCameraId(), trafficEvent.getAverageSpeed());
            PublicAlertEvent alert = new PublicAlertEvent(message, 300, "DISTRICT-A"); // 5 min alert
            logger.log("Publishing Public Alert: " + alert.toString());
            this.eventBus.publish(alert);
        }
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/services/PowerManagementService.java
 * ---------------------------------
 * NEW SERVICE - Listens for power events and action commands.
 */
class PowerManagementService implements Subscriber {
    private final SimpleLogger logger = new SimpleLogger("PowerService");

    public PowerManagementService() {}

    /**
     * Registers this service with the event bus.
     * @param bus The EventBus instance.
     */
    public void register(EventBus bus) {
        bus.register(PowerUsageEvent.class, this);
        bus.register(ActionDispatchEvent.class, this);
    }

    @Override
    public void onEvent(Event event) {
        if (event instanceof PowerUsageEvent e) {
            // In a real system, this would log to a time-series DB.
            // We'll just log if it's high, but not an anomaly.
            if (e.getCurrentDrawAmps() > 50.0) {
                logger.log("High power draw detected: " + e.getCurrentDrawAmps() + "A at " + e.getMeterId());
            }
        } else if (event instanceof ActionDispatchEvent e) {
            if ("LOAD_BALANCE_GRID".equals(e.getActionType())) {
                logger.warn("ACTION RECEIVED: Executing LOAD_BALANCE_GRID on target " + e.getTargetDevice());
                // ... logic to reroute power ...
            } else if ("ACTIVATE_COOLING".equals(e.getActionType())) {
                logger.warn("ACTION RECEIVED: Activating cooling for " + e.getTargetDevice() + " due to high temp.");
                // ... logic to turn on AC ...
            }
        }
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/services/EmergencyDispatchService.java
 * ---------------------------------
 * NEW SERVICE - Listens for emergency calls and dispatches units.
 */
class EmergencyDispatchService implements Subscriber {
    private final SimpleLogger logger = new SimpleLogger("DispatchService");
    private final EventBus eventBus; // To publish new actions

    public EmergencyDispatchService(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    /**
     * Registers this service with the event bus.
     * @param bus The EventBus instance.
     */
    public void register(EventBus bus) {
        bus.register(EmergencyServiceEvent.class, this);
        // Can also listen for anomalies, e.g., a CRITICAL fire anomaly
        // bus.register(AnomalyDetectedEvent.class, this);
    }

    @Override
    public void onEvent(Event event) {
        if (event instanceof EmergencyServiceEvent e) {
            logger.warn("!!! EMERGENCY CALL RECEIVED !!! Dispatching unit...");
            String unitType = "UNIT_TYPE_" + e.getType().name();
            String target = "CLOSEST_" + unitType + "_TO_" + e.getLocation().replace(" ", "_");
            
            Map<String, String> params = Map.of(
                "location", e.getLocation(),
                "severity", String.valueOf(e.getSeverity()),
                "description", e.getDescription()
            );

            ActionDispatchEvent dispatchAction = new ActionDispatchEvent(
                "DISPATCH_EMERGENCY_UNIT",
                target,
                params
            );
            
            // This could be consumed by a "UnitManagementService"
            eventBus.publish(dispatchAction);
        }
    }
}

/**
 * ---------------------------------
 * FILE: com/smartcity/services/PublicDisplayService.java
 * ---------------------------------
 * NEW SERVICE - Listens for public alerts and "displays" them.
 */
class PublicDisplayService implements Subscriber {
    private final SimpleLogger logger = new SimpleLogger("PublicDisplay");

    public PublicDisplayService() {}

    /**
     * Registers this service with the event bus.
     * @param bus The EventBus instance.
     */
    public void register(EventBus bus) {
        bus.register(PublicAlertEvent.class, this);
    }

    @Override
    public void onEvent(Event event) {
        if (event instanceof PublicAlertEvent e) {
            logger.log("--- PUBLIC DISPLAY [Area: " + e.getTargetArea() + "] ---");
            logger.log(">>> " + e.getMessage());
            logger.log("--- (Displaying for " + e.getDurationSeconds() + " seconds) ---");
        }
    }
}


/**
 * ---------------------------------
 * FILE: com.smartcity/services/SystemMonitorService.java
 * ---------------------------------
 * A service that monitors the system and other services.
 * It is both a Runnable (to publish its own health) and
 * a Subscriber (to listen for actions).
 */
class SystemMonitorService implements Subscriber, Runnable {
    private final SimpleLogger logger = new SimpleLogger("SystemMonitor");
    private final EventBus eventBus;
    private final Instant startTime = Instant.now();
    private volatile boolean running = true;

    public SystemMonitorService(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    /**
     * Registers this service with the event bus.
     * @param bus The EventBus instance.
     */
    public void register(EventBus bus) {
        bus.register(ActionDispatchEvent.class, this);
        bus.register(SystemHealthEvent.class, this);
    }

    public void stop() {
        this.running = false;
    }

    /**
     * Subscribes to events to monitor system activity.
     * @param event The event to monitor.
     */
    @Override
    public void onEvent(Event event) {
        if (event instanceof ActionDispatchEvent e) {
            logger.log("MONITORED ACTION: " + e.getActionType() + " on " + e.getTargetDevice());
        } else if (event instanceof SystemHealthEvent e) {
            if (!"OK".equals(e.getStatus())) {
                logger.warn("HEALTH ALERT: Service " + e.getServiceName() + " reporting status: " + e.getStatus());
            }
        }
    }

    /**
     * Runs in its own thread to periodically publish its own health status.
     */
    @Override
    public void run() {
        logger.log("System Monitor started.");
        while (running) {
            try {
                long uptime = ChronoUnit.MILLIS.between(startTime, Instant.now());
                SystemHealthEvent health = new SystemHealthEvent(
                    "SystemMonitorService",
                    "OK",
                    uptime
                );
                eventBus.publish(health);
                Thread.sleep(30000); // Publish health every 30 seconds
            } catch (InterruptedException e) {
                this.running = false;
            }
        }
        logger.log("System Monitor stopped.");
    }
}

/**
 * =======================================================================
 * PACKAGE: com.smartcity.util
 * DESCRIPTION: Utility classes
 * =======================================================================
 */

/**
 * ---------------------------------
 * FILE: com/smartcity/util/SimpleLogger.java
 * ---------------------------------
 * A simple, synchronized logger to make console output readable
 * in a highly concurrent, multi-threaded environment.
 */
class SimpleLogger {
    private final String contextName;

    public SimpleLogger(String contextName) {
        this.contextName = contextName;
    }

    /**
     * Internal logging method.
     * @param level The log level (e.g., "INFO", "WARN").
     * @param message The message to log.
     */
    private void logInternal(String level, String message) {
        String threadName = Thread.currentThread().getName();
        // Synchronize on System.out to prevent interleaved lines
        synchronized(System.out) {
            System.out.printf("%s [%-7s] [%-22s] [%-20s] %s%n",
                Instant.now().toString(),
                level,
                threadName,
                contextName,
                message
            );
        }
    }

    public void log(String message) {
        logInternal("INFO", message);
    }
    
    public void debug(String message) {
        logInternal("DEBUG", message);
    }

    public void warn(String message) {
        logInternal("WARN", message);
    }

    public void error(String message, Throwable t) {
        logInternal("ERROR", message);
        if (t != null) {
            synchronized(System.out) {
                t.printStackTrace(System.out);
            }
        }
    }
}


/**
 * =======================================================================
 * PACKAGE: com.smartcity
 * DESCRIPTION: Main application entry point.
 * =======================================================================
 */

/**
 * ---------------------------------
 * FILE: com/smartcity/Main.java
 * ---------------------------------
 * Main class to wire up and run the simulation.
 * This acts as the "dependency injection" root.
 */
public class SmartCitySimulation {

    private static final SimpleLogger mainLogger = new SimpleLogger("Application");

    /**
     * Main entry point.
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        mainLogger.log("=======================================");
        mainLogger.log("=  Starting Smart City Simulation...  =");
        mainLogger.log("=======================================");

        // 1. Create the central EventBus
        EventBus eventBus = new EventBus();

        // 2. Create all services (subscribers)
        mainLogger.log("Initializing services...");
        LoggingService loggingService = new LoggingService();
        DataAnalyticsService analyticsService = new DataAnalyticsService();
        AnomalyDetectionService anomalyService = new AnomalyDetectionService(eventBus);
        ControlCenterService controlService = new ControlCenterService(eventBus);
        SystemMonitorService monitorService = new SystemMonitorService(eventBus);
        PowerManagementService powerService = new PowerManagementService();
        EmergencyDispatchService dispatchService = new EmergencyDispatchService(eventBus);
        PublicDisplayService displayService = new PublicDisplayService();

        // 3. Register services with the EventBus
        mainLogger.log("Registering services with EventBus...");
        loggingService.register(eventBus);
        analyticsService.register(eventBus);
        anomalyService.register(eventBus);
        controlService.register(eventBus);
        monitorService.register(eventBus);
        powerService.register(eventBus);
        dispatchService.register(eventBus);
        displayService.register(eventBus);

        // 4. Create sensor simulators (publishers)
        mainLogger.log("Creating sensor simulators...");
        IotSensorSimulator weatherSensor1 = new IotSensorSimulator(eventBus, "SENSOR-DISTRICT-A");
        IotSensorSimulator weatherSensor2 = new IotSensorSimulator(eventBus, "SENSOR-DISTRICT-B");
        TrafficCameraSimulator trafficCam1 = new TrafficCameraSimulator(eventBus, "CAM-MAIN-ST-01");
        SmartMeterSimulator meter1 = new SmartMeterSimulator(eventBus, "METER-BLDG-100");
        EmergencyCallSimulator callSimulator = new EmergencyCallSimulator(eventBus);

        // 5. Start all runnable components in their own threads
        mainLogger.log("Starting all threads...");
        new Thread(analyticsService::startReporting, "Analytics-Start").start(); // Starts the scheduled reporter
        new Thread(monitorService, "Monitor-Thread").start();
        new Thread(weatherSensor1, "Sensor-A-Thread").start();
        new Thread(weatherSensor2, "Sensor-B-Thread").start();
        new Thread(trafficCam1, "Camera-1-Thread").start();
        new Thread(meter1, "Meter-1-Thread").start();
        new Thread(callSimulator, "911-Sim-Thread").start();

        // 6. Add a shutdown hook to gracefully stop the simulation
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            mainLogger.log("=======================================");
            mainLogger.log("=  Shutdown signal received...        =");
            mainLogger.log("=  Stopping simulators and services.  =");
            mainLogger.log("=======================================");
            weatherSensor1.stop();
            weatherSensor2.stop();
            trafficCam1.stop();
            meter1.stop();
            callSimulator.stop();
            monitorService.stop();
            analyticsService.stopReporting();
            eventBus.shutdown();
            mainLogger.log("=  Simulation shut down. Exiting.   =");
            mainLogger.log("=======================================");
        }, "Shutdown-Hook"));

        mainLogger.log("Simulation is running. (Press Ctrl+C to stop)");
    }
}