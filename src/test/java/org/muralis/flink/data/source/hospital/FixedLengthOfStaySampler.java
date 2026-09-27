package org.muralis.flink.data.source.hospital;

import java.io.Serial;

/** Test {@link LengthOfStaySampler} that gives every patient the same length of stay. */
final class FixedLengthOfStaySampler implements LengthOfStaySampler {

    @Serial
    private static final long serialVersionUID = 1L;

    private final long lengthOfStayMillis;

    FixedLengthOfStaySampler(long lengthOfStayMillis) {
        this.lengthOfStayMillis = lengthOfStayMillis;
    }

    @Override
    public long lengthOfStayMillis(String patientID, String regionID) {
        return lengthOfStayMillis;
    }
}
