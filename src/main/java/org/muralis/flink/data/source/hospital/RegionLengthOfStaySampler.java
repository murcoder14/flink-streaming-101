package org.muralis.flink.data.source.hospital;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * {@link LengthOfStaySampler} whose length-of-stay range depends on the hospital's region, which
 * is how "some regions discharge slower" is modelled.
 *
 * <p>Each region has a {@link Profile}: a plain {@code [min, max]} range of real-world durations,
 * sampled uniformly. The random draw is seeded from the {@code patientID}, so the same patient
 * always gets the same length of stay (see {@link LengthOfStaySampler}).
 *
 * <p>The ranges are sized with Little's law {@code L = λ · meanLOS} against the default admission
 * rate of 2/s (see {@code docs/discharge_event_plan.md}):
 * <ul>
 *   <li>Regions <b>NE</b>, <b>MW</b>, <b>W</b>: {@link #NORMAL_MIN_LOS}-{@link #NORMAL_MAX_LOS}
 *       (mean 10 min), so steady-state occupancy stays below the 90% alert threshold</li>
 *   <li>Region <b>S</b>: {@link #SLOW_MIN_LOS}-{@link #SLOW_MAX_LOS} (mean 40 min), so
 *       steady-state occupancy (~120%) runs out of beds</li>
 * </ul>
 *
 * <p>To change the pace of a demo, edit these {@link Duration} constants directly.
 */
public class RegionLengthOfStaySampler implements LengthOfStaySampler {

    @Serial
    private static final long serialVersionUID = 1L;

    public static final Duration NORMAL_MIN_LOS = Duration.ofMinutes(5);
    public static final Duration NORMAL_MAX_LOS = Duration.ofMinutes(15);
    public static final Duration SLOW_MIN_LOS = Duration.ofMinutes(20);
    public static final Duration SLOW_MAX_LOS = Duration.ofMinutes(60);

    private final Map<String, Profile> profilesByRegion;
    private final Profile fallbackProfile;

    /**
     * @param profilesByRegion length-of-stay profile per region ID
     * @param fallbackProfile  profile used for a {@code null} or unmapped region
     */
    public RegionLengthOfStaySampler(Map<String, Profile> profilesByRegion, Profile fallbackProfile) {
        this.profilesByRegion = Collections.unmodifiableMap(new HashMap<>(profilesByRegion));
        this.fallbackProfile = fallbackProfile;
    }

    /** Default regional profiles. */
    public static RegionLengthOfStaySampler defaults() {
        Profile normal = new Profile(NORMAL_MIN_LOS.toMillis(), NORMAL_MAX_LOS.toMillis());
        Profile slow = new Profile(SLOW_MIN_LOS.toMillis(), SLOW_MAX_LOS.toMillis());

        Map<String, Profile> profiles = new HashMap<>();
        profiles.put(PatientAdmissionGeneratorFunction.REGION_NE, normal);
        profiles.put(PatientAdmissionGeneratorFunction.REGION_MW, normal);
        profiles.put(PatientAdmissionGeneratorFunction.REGION_W, normal);
        profiles.put(PatientAdmissionGeneratorFunction.REGION_S, slow);
        return new RegionLengthOfStaySampler(profiles, normal);
    }

    @Override
    public long lengthOfStayMillis(String patientID, String regionID) {
        Profile profile = regionID != null ? profilesByRegion.getOrDefault(regionID, fallbackProfile) : fallbackProfile;
        return profile.sample(new SplittableRandom(seedFrom(patientID)));
    }

    /** Returns the profile used for {@code regionID}. */
    public Profile profileFor(String regionID) {
        return regionID != null ? profilesByRegion.getOrDefault(regionID, fallbackProfile) : fallbackProfile;
    }

    /**
     * {@link String#hashCode()} is specified by the JLS, so the seed is identical on every JVM
     * (unlike {@link Object#hashCode()}), which keeps the sampler deterministic across TaskManagers.
     */
    private static long seedFrom(String patientID) {
        return patientID != null ? patientID.hashCode() : 0L;
    }

    /** Length of stay drawn uniformly from {@code [minMillis, maxMillis]}. */
    public record Profile(long minMillis, long maxMillis) implements Serializable {

        public Profile {
            if (minMillis <= 0 || minMillis > maxMillis) {
                throw new IllegalArgumentException(
                        "require 0 < min <= max, got min=" + minMillis + ", max=" + maxMillis);
            }
        }

        long sample(SplittableRandom random) {
            return minMillis == maxMillis ? minMillis : minMillis + random.nextLong(maxMillis - minMillis + 1);
        }
    }
}
