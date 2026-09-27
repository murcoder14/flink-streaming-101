package org.muralis.flink.data.source.hospital;

import java.io.Serializable;

/**
 * Decides how long an admitted patient stays in hospital before being discharged.
 *
 * <p>Implementations must be <b>pure</b>: the same {@code (patientID, regionID)} must always yield
 * the same length of stay. On failure recovery Flink replays admissions from the last checkpoint,
 * and a pure sampler guarantees the replayed admission schedules the same discharge time as the
 * original one did. It also lets unit tests assert exact discharge timestamps.
 *
 * <p>The sampler is shipped to the cluster as part of the function that uses it, so it must be
 * {@link Serializable}.
 */
@FunctionalInterface
public interface LengthOfStaySampler extends Serializable {

    /**
     * @param patientID the admitted patient's ID
     * @param regionID  the region of the admitting hospital ({@code null} if the hospital is unknown)
     * @return length of stay in milliseconds, always {@code > 0}
     */
    long lengthOfStayMillis(String patientID, String regionID);
}
