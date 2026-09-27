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
 * Result model representing the number of patient admissions within a time window for a specific region.
 * Follows Flink POJO conventions.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class RegionalAdmissionCount implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    private String regionID;
    private long count;
    private long windowStart;
    private long windowEnd;

    @Override
    public String toString() {
        return String.format("[%s - %s] Region %-2s -> %3d admissions",
                TIME.format(Instant.ofEpochMilli(windowStart)),
                TIME.format(Instant.ofEpochMilli(windowEnd)),
                regionID,
                count);
    }
}
