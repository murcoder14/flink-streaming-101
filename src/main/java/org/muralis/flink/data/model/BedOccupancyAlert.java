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
 * Represents a change in a hospital's bed-occupancy status (e.g. {@code NORMAL -> HIGH} or
 * {@code FULL -> HIGH}). Unlike {@link CapacityAlert}, which fires once on cumulative admissions,
 * this alert is raised on every status transition, in both directions, because occupancy goes
 * down as well as up once discharges are modelled.
 *
 * <p>Follows Flink POJO conventions for serialization and state management.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class BedOccupancyAlert implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    private String hospitalID;
    private String regionID;
    private BedOccupancyStatus previousStatus;
    private BedOccupancyStatus status;
    private int occupied;
    private int maxCapacity;
    private double occupancyPercentage;
    /** Event time: epoch milliseconds of the admission or discharge that caused the transition. */
    private long alertTime;

    @Override
    public String toString() {
        return String.format("[%s] %s -> %s: Hospital %s (Region %s) at %d/%d beds (%.1f%% occupancy)",
                TIME.format(Instant.ofEpochMilli(alertTime)),
                previousStatus,
                status,
                hospitalID,
                regionID,
                occupied,
                maxCapacity,
                occupancyPercentage);
    }
}
