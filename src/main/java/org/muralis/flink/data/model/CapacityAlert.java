package org.muralis.flink.data.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Represents a bed-capacity alert raised when a hospital's cumulative occupancy crosses
 * the configured alert threshold of its maximum bed capacity.
 * Follows Flink POJO conventions for serialization and state management.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class CapacityAlert implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    private String hospitalID;
    private String regionID;
    private long currentOccupancy;
    private int maxCapacity;
    private double occupancyPercentage;
    /** Epoch milliseconds when the alert was raised. */
    private long alertTime;

    @Override
    public String toString() {
        return String.format("[%s] ALERT: Hospital %s (Region %s) at %d/%d beds (%.1f%% capacity)",
                TIME.format(Instant.ofEpochMilli(alertTime)),
                hospitalID,
                regionID,
                currentOccupancy,
                maxCapacity,
                occupancyPercentage);
    }
}
