package org.muralis.flink.data.source.hospital;

import java.util.List;

import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegionLengthOfStaySamplerTest {

    private static final int SAMPLES = 20_000;
    private final RegionLengthOfStaySampler sampler = RegionLengthOfStaySampler.defaults();

    @Test
    void samePatientAlwaysGetsTheSameLengthOfStay() {
        for (int i = 0; i < 100; i++) {
            String patientID = patientID(i);
            assertEquals(sampler.lengthOfStayMillis(patientID, "S"), sampler.lengthOfStayMillis(patientID, "S"));
        }
    }

    @Test
    void isStillDeterministicAfterJavaSerialization() throws Exception {
        // Flink ships the sampler to TaskManagers by serializing it.
        RegionLengthOfStaySampler copy = InstantiationUtil.clone(sampler);
        for (int i = 0; i < 100; i++) {
            assertEquals(sampler.lengthOfStayMillis(patientID(i), "NE"), copy.lengthOfStayMillis(patientID(i), "NE"));
        }
    }

    @Test
    void regionSDischargesSlowerThanEveryOtherRegion() {
        double meanS = sampleMean("S");
        for (String region : List.of("NE", "MW", "W")) {
            assertTrue(meanS > 3 * sampleMean(region), "S mean " + meanS + " vs " + region + " " + sampleMean(region));
        }
    }

    @Test
    void sampleMeanIsCloseToTheMidpointOfTheRange() {
        double expected = (RegionLengthOfStaySampler.SLOW_MIN_LOS.toMillis() + RegionLengthOfStaySampler.SLOW_MAX_LOS.toMillis()) / 2.0;
        assertEquals(expected, sampleMean("S"), expected * 0.05);
    }

    @Test
    void everyValueIsWithinItsConfiguredRange() {
        for (String region : new String[] {"NE", "MW", "W", "S", "unknown", null}) {
            RegionLengthOfStaySampler.Profile profile = sampler.profileFor(region);
            for (int i = 0; i < SAMPLES; i++) {
                long los = sampler.lengthOfStayMillis(patientID(i), region);
                assertTrue(los >= profile.minMillis() && los <= profile.maxMillis(), region + ": " + los);
            }
        }
    }

    private double sampleMean(String region) {
        long total = 0;
        for (int i = 0; i < SAMPLES; i++) {
            total += sampler.lengthOfStayMillis(patientID(i), region);
        }
        return (double) total / SAMPLES;
    }

    private static String patientID(int i) {
        return String.format("PAT-%06d", 100000 + i);
    }
}
