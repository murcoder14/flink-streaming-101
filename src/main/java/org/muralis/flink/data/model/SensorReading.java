package org.muralis.flink.data.model;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * The record that flows through every lesson: one temperature measurement of one sensor.
 * Flink can process any Java class, but it is fastest (and its state stays upgradeable) when the class is a "POJO":
 *   - the class is public
 *   - it has a public constructor without arguments
 *   - every field is public (or has a getter and a setter)
 * This class follows those rules on purpose.
 */
public class SensorReading {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    public String sensorId;
    /** Event time: when the measurement happened, in epoch milliseconds. */
    public long timestamp;
    /** Degrees Celsius. The value -999 marks a faulty measurement. */
    public double temperature;

    public SensorReading() { }

    public SensorReading(String sensorId, long timestamp, double temperature) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return sensorId + " " + temperature + "C @" + TIME.format(Instant.ofEpochMilli(timestamp));
    }
}

