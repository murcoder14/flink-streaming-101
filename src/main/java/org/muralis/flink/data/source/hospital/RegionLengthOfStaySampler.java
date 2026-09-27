package org.muralis.flink.data.source.hospital;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * {@link LengthOfStaySampler} whose length-of-stay distribution depends on the hospital's region,
 * which is how "some regions discharge slower" is modelled.
 *
 * <p>Each region has a {@link Profile}: an exponential distribution with the region's mean,
 * clamped to {@code [min, max]} so that the timer horizon (and therefore state size) is bounded.
 * The random draw is seeded from the {@code patientID}, so the same patient always gets the same
 * length of stay (see {@link LengthOfStaySampler}).
 *
 * <p>Default means (at {@code losScale = 1.0}), sized with Little's law {@code L = λ · meanLOS}
 * against the default admission rate of 2/s (see {@code docs/discharge_event_plan.md}):
 * <ul>
 *   <li>Regions <b>NE</b>, <b>MW</b>, <b>W</b>: 10 minutes, so steady-state occupancy stays below
 *       the 90% alert threshold</li>
 *   <li>Region <b>S</b>: 40 minutes, so steady-state occupancy (~120%) runs out of beds</li>
 * </ul>
 */
public class RegionLengthOfStaySampler implements LengthOfStaySampler {

    @Serial
    private static final long serialVersionUID = 1L;

    public static final Duration DEFAULT_MEAN_LOS = Duration.ofMinutes(10);
    public static final Duration SLOW_REGION_MEAN_LOS = Duration.ofMinutes(40);

    /** Lower clamp as a fraction of the mean. */
    private static final double MIN_FRACTION_OF_MEAN = 0.1;
    /** Upper clamp as a multiple of the mean; bounds how far in the future a timer can be. */
    private static final double MAX_MULTIPLE_OF_MEAN = 5.0;

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

    /** Default regional profiles at real-time speed. */
    public static RegionLengthOfStaySampler defaults() {
        return defaults(1.0);
    }

    /**
     * Default regional profiles with every mean multiplied by {@code losScale}. Scaling the
     * admission rate by {@code k} and the length of stay by {@code 1/k} keeps every steady-state
     * occupancy the same but reaches it {@code k} times faster.
     */
    public static RegionLengthOfStaySampler defaults(double losScale) {
        if (!(losScale > 0)) {
            throw new IllegalArgumentException("losScale must be > 0, was " + losScale);
        }
        Profile normal = Profile.withMean(scale(DEFAULT_MEAN_LOS, losScale));
        Profile slow = Profile.withMean(scale(SLOW_REGION_MEAN_LOS, losScale));

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

    private static long scale(Duration duration, double factor) {
        return Math.max(1L, Math.round(duration.toMillis() * factor));
    }

    /**
     * Exponentially distributed length of stay, clamped to {@code [minMillis, maxMillis]}.
     *
     * @param meanMillis mean of the (unclamped) exponential distribution
     * @param minMillis  shortest possible stay, {@code >= 1}
     * @param maxMillis  longest possible stay
     */
    public record Profile(long meanMillis, long minMillis, long maxMillis) implements Serializable {

        public Profile {
            if (meanMillis <= 0 || minMillis <= 0 || minMillis > maxMillis) {
                throw new IllegalArgumentException(
                        "require mean > 0 and 0 < min <= max, got mean=" + meanMillis + ", min=" + minMillis + ", max=" + maxMillis);
            }
        }

        /** Profile with the default clamp bounds: {@code [0.1 × mean, 5 × mean]}. */
        public static Profile withMean(long meanMillis) {
            long min = Math.max(1L, Math.round(meanMillis * MIN_FRACTION_OF_MEAN));
            long max = Math.max(min, Math.round(meanMillis * MAX_MULTIPLE_OF_MEAN));
            return new Profile(meanMillis, min, max);
        }

        long sample(SplittableRandom random) {
            // Inverse-CDF sampling: 1 - nextDouble() is in (0, 1], so the log is always finite.
            double exponential = -meanMillis * Math.log(1.0 - random.nextDouble());
            long los = Math.round(exponential);
            return Math.min(maxMillis, Math.max(minMillis, los));
        }
    }
}
