package org.muralis.flink.data.model;

/**
 * Bed-occupancy level of a single hospital. Transitions between levels use hysteresis
 * (see {@code HospitalBedOccupancyMonitor}) so that occupancy hovering around a boundary does
 * not produce an alert storm.
 */
public enum BedOccupancyStatus {
    /** Occupancy comfortably below the alert threshold. */
    NORMAL,
    /** Occupancy at or above the alert threshold (90% of capacity). */
    HIGH,
    /** Every bed is occupied (100% of capacity or more). */
    FULL
}
