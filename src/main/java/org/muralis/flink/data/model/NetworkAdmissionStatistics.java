package org.muralis.flink.data.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Result model representing the total admissions across all 4 regions (NE, MW, W, S) within a time window,
 * including a regional breakdown.
 * Follows Flink POJO conventions.
 */
@Getter
@NoArgsConstructor
@EqualsAndHashCode
public class NetworkAdmissionStatistics implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    @Setter
    private long windowStart;
    @Setter
    private long windowEnd;
    @Setter
    private long totalAdmissions;
    private Map<String, Long> regionCounts = new LinkedHashMap<>();

    public NetworkAdmissionStatistics(long windowStart, long windowEnd, long totalAdmissions, Map<String, Long> regionCounts) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.totalAdmissions = totalAdmissions;
        this.regionCounts = regionCounts != null ? new LinkedHashMap<>(regionCounts) : new LinkedHashMap<>();
    }

    // Hand-written: defensively copies the map, so it is not a plain @Setter.
    public void setRegionCounts(Map<String, Long> regionCounts) {
        this.regionCounts = regionCounts != null ? new LinkedHashMap<>(regionCounts) : new LinkedHashMap<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%n==================================================%n"));
        sb.append(String.format("  Admission Interval [%s - %s]%n",
                TIME.format(Instant.ofEpochMilli(windowStart)),
                TIME.format(Instant.ofEpochMilli(windowEnd))));
        sb.append(String.format("  Total Admissions Across All 4 Regions: %d%n", totalAdmissions));
        sb.append(String.format("  ------------------------------------------------%n"));
        regionCounts.forEach((region, count) -> {
            double pct = totalAdmissions > 0 ? (count * 100.0 / totalAdmissions) : 0.0;
            sb.append(String.format("    - Region %-2s: %3d admissions (%5.1f%%)%n", region, count, pct));
        });
        sb.append("==================================================");
        return sb.toString();
    }
}
