package org.muralis.flink.data.source.hospital;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests the static hospital/region/capacity lookups in {@link HospitalCapacityRegistry}. */
class HospitalCapacityRegistryTest {

    @Test
    void resolvesRegionForKnownHospitals() {
        assertEquals("NE", HospitalCapacityRegistry.regionForHospital("HNE1"));
        assertEquals("MW", HospitalCapacityRegistry.regionForHospital("HMW3"));
        assertEquals("W", HospitalCapacityRegistry.regionForHospital("HW5"));
        assertEquals("S", HospitalCapacityRegistry.regionForHospital("HS2"));
    }

    @Test
    void returnsNullRegionForUnknownHospital() {
        assertNull(HospitalCapacityRegistry.regionForHospital("DOES-NOT-EXIST"));
    }

    @Test
    void resolvesMaxBedCapacityPerRegion() {
        assertEquals(200, HospitalCapacityRegistry.capacityForHospital("HNE1"));
        assertEquals(170, HospitalCapacityRegistry.capacityForHospital("HMW2"));
        assertEquals(300, HospitalCapacityRegistry.capacityForHospital("HW4"));
        assertEquals(100, HospitalCapacityRegistry.capacityForHospital("HS1"));
    }

    @Test
    void returnsNegativeOneCapacityForUnknownHospital() {
        assertEquals(-1, HospitalCapacityRegistry.capacityForHospital("DOES-NOT-EXIST"));
    }

    @Test
    void alertThresholdRatioIsNinetyPercent() {
        assertEquals(0.9, HospitalCapacityRegistry.ALERT_THRESHOLD_RATIO, 0.0001);
    }
}
