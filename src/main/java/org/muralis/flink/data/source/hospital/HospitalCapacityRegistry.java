package org.muralis.flink.data.source.hospital;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static lookup of hospital bed capacity and hospital-to-region membership.
 *
 * <p>Maximum bed capacity is defined per region and applies to every hospital in that region:
 * <ul>
 *   <li>Region <b>NE</b>: 200 beds</li>
 *   <li>Region <b>MW</b>: 170 beds</li>
 *   <li>Region <b>W</b>:  300 beds</li>
 *   <li>Region <b>S</b>:  500 beds</li>
 * </ul>
 */
public final class HospitalCapacityRegistry {

    /** An alert fires once occupancy reaches this fraction of a hospital's maximum capacity. */
    public static final double ALERT_THRESHOLD_RATIO = 0.9;

    private static final Map<String, Integer> REGION_MAX_BED_CAPACITY;
    private static final Map<String, String> HOSPITAL_TO_REGION;

    static {
        Map<String, Integer> capacities = new HashMap<>();
        capacities.put(PatientAdmissionGeneratorFunction.REGION_NE, 200);
        capacities.put(PatientAdmissionGeneratorFunction.REGION_MW, 170);
        capacities.put(PatientAdmissionGeneratorFunction.REGION_W, 300);
        capacities.put(PatientAdmissionGeneratorFunction.REGION_S, 500);
        REGION_MAX_BED_CAPACITY = Collections.unmodifiableMap(capacities);

        Map<String, String> hospitalToRegion = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : PatientAdmissionGeneratorFunction.REGION_HOSPITALS.entrySet()) {
            String regionID = entry.getKey();
            for (String hospitalID : entry.getValue()) {
                hospitalToRegion.put(hospitalID, regionID);
            }
        }
        HOSPITAL_TO_REGION = Collections.unmodifiableMap(hospitalToRegion);
    }

    private HospitalCapacityRegistry() {}

    /** Returns the region a hospital belongs to, or {@code null} if the hospital is unknown. */
    public static String regionForHospital(String hospitalID) {
        return HOSPITAL_TO_REGION.get(hospitalID);
    }

    /** Returns the maximum bed capacity for the hospital's region, or {@code -1} if unknown. */
    public static int capacityForHospital(String hospitalID) {
        String regionID = regionForHospital(hospitalID);
        return regionID != null ? REGION_MAX_BED_CAPACITY.getOrDefault(regionID, -1) : -1;
    }
}
