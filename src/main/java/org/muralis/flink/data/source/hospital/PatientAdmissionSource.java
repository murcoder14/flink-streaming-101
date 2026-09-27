package org.muralis.flink.data.source.hospital;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.muralis.flink.data.model.AdmitEvent;

/**
 * Flink Source factory providing ready-to-use {@link DataGeneratorSource} and {@link DataStream}
 * for simulated patient admissions across hospital regions.
 *
 * <p>Uses {@link RateLimiterStrategy#perSecond(double)} configured to 2 admissions per second by default.
 */
public final class PatientAdmissionSource {

    /** Default throughput: 2 admissions per second as required. */
    public static final double DEFAULT_RATE_PER_SECOND = 2.0;

    private PatientAdmissionSource() {}

    /**
     * Creates a {@link DataGeneratorSource} using {@link PatientAdmissionGeneratorFunction}
     * with {@link RateLimiterStrategy#perSecond(double)} set to 2.
     *
     * @return Flink FLIP-27 data generator source emitting {@link AdmitEvent} records.
     */
    public static DataGeneratorSource<AdmitEvent> createSource() {
        return createSource(DEFAULT_RATE_PER_SECOND);
    }

    /**
     * Creates a {@link DataGeneratorSource} using {@link PatientAdmissionGeneratorFunction}
     * with the specified rate limiter per second.
     *
     * @param admissionsPerSecond throughput rate in records/second.
     * @return Flink FLIP-27 data generator source.
     */
    public static DataGeneratorSource<AdmitEvent> createSource(double admissionsPerSecond) {
        return new DataGeneratorSource<>(
                new PatientAdmissionGeneratorFunction(),
                Long.MAX_VALUE, // Unbounded stream
                RateLimiterStrategy.perSecond(admissionsPerSecond),
                TypeInformation.of(AdmitEvent.class)
        );
    }

    /**
     * Creates a {@link DataStream} of {@link AdmitEvent} admissions at the default rate of 2 events/second.
     *
     * @param env StreamExecutionEnvironment
     * @return bounded/unbounded DataStream of AdmitEvent records
     */
    public static DataStream<AdmitEvent> admissions(StreamExecutionEnvironment env) {
        return admissions(env, DEFAULT_RATE_PER_SECOND);
    }

    /**
     * Creates a {@link DataStream} of {@link AdmitEvent} admissions with a custom rate per second.
     *
     * @param env StreamExecutionEnvironment
     * @param admissionsPerSecond rate of admissions per second
     * @return DataStream of AdmitEvent records
     */
    public static DataStream<AdmitEvent> admissions(StreamExecutionEnvironment env, double admissionsPerSecond) {
        DataGeneratorSource<AdmitEvent> source = createSource(admissionsPerSecond);

        WatermarkStrategy<AdmitEvent> watermarks = WatermarkStrategy
                .<AdmitEvent>forMonotonousTimestamps()
                .withTimestampAssigner((event, prev) -> event.getAdmitTime());

        return env.fromSource(source, watermarks, "patient-admissions-source");
    }
}
