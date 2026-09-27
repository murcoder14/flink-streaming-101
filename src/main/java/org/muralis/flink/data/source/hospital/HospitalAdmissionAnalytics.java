package org.muralis.flink.data.source.hospital;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.muralis.flink.data.model.AdmitEvent;
import org.muralis.flink.data.model.NetworkAdmissionStatistics;
import org.muralis.flink.data.model.RegionalAdmissionCount;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Analytics and windowing transformations to measure patient admissions across all 4 regions.
 */
public final class HospitalAdmissionAnalytics {

    /** Default measurement interval: 2 minutes. */
    public static final Duration DEFAULT_INTERVAL = Duration.ofMinutes(2);

    private HospitalAdmissionAnalytics() {}

    /**
     * Measures how many admissions are occurring in each 2-minute interval grouped by region.
     *
     * @param stream stream of AdmitEvent records
     * @return stream of RegionalAdmissionCount for each region
     */
    public static DataStream<RegionalAdmissionCount> measureByRegion(DataStream<AdmitEvent> stream) {
        return measureByRegion(stream, DEFAULT_INTERVAL);
    }

    /**
     * Measures how many admissions are occurring in the specified interval grouped by region.
     * Uses incremental aggregation (AdmissionCountAggregator) combined with ProcessWindowFunction
     * for optimal performance and minimal state overhead.
     *
     * @param stream stream of AdmitEvent records
     * @param interval window duration
     * @return stream of RegionalAdmissionCount
     */
    public static DataStream<RegionalAdmissionCount> measureByRegion(DataStream<AdmitEvent> stream,Duration interval) {

        return stream
                .keyBy(AdmitEvent::getRegionID)
                .window(TumblingProcessingTimeWindows.of(interval))
                .aggregate(
                        new AdmissionCountAggregator(),
                        new RegionalAdmissionWindowFunction(),
                        TypeInformation.of(Long.class),
                        TypeInformation.of(Long.class),
                        TypeInformation.of(RegionalAdmissionCount.class)
                );
    }

    /**
     * Measures how many admissions are occurring in the specified interval grouped by region.
     * Uses incremental aggregation (AdmissionCountAggregator) combined with ProcessWindowFunction
     * for optimal performance and minimal state overhead.
     *
     * @param stream stream of AdmitEvent records
     * @param regionID the ID of the region to filter by
     * @param interval window duration
     * @return stream of RegionalAdmissionCount
     */
    public static DataStream<RegionalAdmissionCount> measureByRegion(DataStream<AdmitEvent> stream,String regionID, Duration interval) {

        return stream
                .filter(event -> event.getRegionID().equals(regionID))
                .keyBy(AdmitEvent::getRegionID)
                .window(TumblingProcessingTimeWindows.of(interval))
                .aggregate(
                        new AdmissionCountAggregator(),
                        new RegionalAdmissionWindowFunction(),
                        TypeInformation.of(Long.class),
                        TypeInformation.of(Long.class),
                        TypeInformation.of(RegionalAdmissionCount.class)
                );
    }
    /**
     * Measures how many admissions are occurring across all 4 regions combined in a 2-minute interval.
     *
     * @param stream stream of AdmitEvent records
     * @return stream of NetworkAdmissionStatistics containing network total and regional breakdown
     */
    public static DataStream<NetworkAdmissionStatistics> measureAcrossAllRegions(DataStream<AdmitEvent> stream) {
        return measureAcrossAllRegions(stream, DEFAULT_INTERVAL);
    }

    /**
     * Measures how many admissions are occurring across all 4 regions in the specified interval.
     *
     * @param stream stream of AdmitEvent records
     * @param interval window duration
     * @return stream of NetworkAdmissionStatistics
     */
    public static DataStream<NetworkAdmissionStatistics> measureAcrossAllRegions(DataStream<AdmitEvent> stream,Duration interval) {
        return stream.windowAll(TumblingProcessingTimeWindows.of(interval)).process(new AllRegionsAdmissionWindowFunction());
    }

    /**
     * Attaches a {@link HospitalCapacityMonitor} to the stream, keyed by {@code hospitalID}.
     * Every input event is forwarded unchanged on the returned stream's main output; capacity
     * alerts (raised once a hospital's cumulative admissions reach
     * {@link HospitalCapacityRegistry#ALERT_THRESHOLD_RATIO} of its maximum bed capacity) are
     * published on {@link HospitalCapacityMonitor#CAPACITY_ALERT_TAG} and can be retrieved via
     * {@code SingleOutputStreamOperator.getSideOutput(HospitalCapacityMonitor.CAPACITY_ALERT_TAG)}.
     *
     * @param stream stream of AdmitEvent records
     * @return the same AdmitEvent records, annotated with a capacity-alert side output
     */
    public static SingleOutputStreamOperator<AdmitEvent> attachCapacityMonitor(DataStream<AdmitEvent> stream) {
        return stream
                .keyBy(AdmitEvent::getHospitalID)
                .process(new HospitalCapacityMonitor());
    }

    /**
     * Attaches a {@link PatientLifecycleSimulator} to the stream, keyed by {@code patientID}.
     * Every input event is forwarded unchanged on the returned stream's main output; each
     * patient's discharge (after a length of stay drawn from {@code lengthOfStaySampler}) is
     * published on {@link PatientLifecycleSimulator#DISCHARGE_TAG} and can be retrieved via
     * {@code SingleOutputStreamOperator.getSideOutput(PatientLifecycleSimulator.DISCHARGE_TAG)}.
     *
     * <p>The stream must carry event-time timestamps (as {@link PatientAdmissionSource} does).
     *
     * @param stream stream of AdmitEvent records
     * @param lengthOfStaySampler decides how long each patient stays
     * @return the same AdmitEvent records, annotated with a discharge side output
     */
    public static SingleOutputStreamOperator<AdmitEvent> attachPatientLifecycleSimulator(DataStream<AdmitEvent> stream, LengthOfStaySampler lengthOfStaySampler) {
        return stream
                .keyBy(admitEvent -> admitEvent.getPatient().getPatientID())
                .process(new PatientLifecycleSimulator(lengthOfStaySampler))
                .uid("patient-lifecycle-simulator")
                .name("patient-lifecycle-simulator");
    }

    // ---------------------------------------------------------------------------------------------
    // Window Functions
    // ---------------------------------------------------------------------------------------------

    /**
     * Incremental accumulator that counts patient admissions efficiently without buffering events.
     */
    public static class AdmissionCountAggregator implements AggregateFunction<AdmitEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        /**        (non-Javadoc)
         * How many senior citizen admissions are occuring in a 2 minute interval only in the region ID "S"?
         * How many female admissions are occuring in a 2 minute interval only in the region ID "MW"?
         * 
         * @see org.apache.flink.api.common.functions.AggregateFunction#add(java.lang.Object, java.lang.Object)
         */
        @Override
        public Long add(AdmitEvent admitEvent, Long accumulator) {
            // Count total admissions:
            // return accumulator + 1;

            // Count only senior citizen (over 65) admissions.
            long admissions = admitEvent.getPatient().getAge() > 65 ? 1 : 0;

            // Count female admissions as well:
            // if (admitEvent.getPatient().getGender().equals("F")) { admissions++; }

            return accumulator + admissions;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * Attaches window metadata (start/end timestamps and region key) to the aggregated count.
     */
    public static class RegionalAdmissionWindowFunction extends ProcessWindowFunction<Long, RegionalAdmissionCount, String, TimeWindow> {

        @Override
        public void process(String regionID,Context context,Iterable<Long> elements,Collector<RegionalAdmissionCount> out) {
            long count = elements.iterator().hasNext() ? elements.iterator().next() : 0L;
            out.collect(new RegionalAdmissionCount(regionID,count,context.window().getStart(),context.window().getEnd()));
        }
    }

    /**
     * ProcessAllWindowFunction that computes both the total admissions and regional breakdown
     * across all 4 regions (NE, MW, W, S) for each window interval.
     * 
     * How many admissions are occuring in a 2 minute interval across all the 4 regions?
     * How many senior citizen admissions are occuring in a 2 minute interval across all the 4 regions?
     * How many female admissions are occuring in a 2 minute interval across all the 4 regions?
     */
    public static class AllRegionsAdmissionWindowFunction extends ProcessAllWindowFunction<AdmitEvent, NetworkAdmissionStatistics, TimeWindow> {

        @Override
        public void process(Context context,Iterable<AdmitEvent> elements,Collector<NetworkAdmissionStatistics> out) {

            long totalAdmissions = 0L;
            Map<String, Long> counts = new LinkedHashMap<>();
            counts.put("NE", 0L);
            counts.put("MW", 0L);
            counts.put("W", 0L);
            counts.put("S", 0L);

            for (AdmitEvent admitEvent : elements) {
                // Count total admissions:
                // long admissions = 1;

                // Count only senior citizen (over 65) admissions.
                long admissions = admitEvent.getPatient().getAge() > 65 ? 1 : 0;

                // Count female admissions as well:
                // if (admitEvent.getPatient().getGender().equals("F")) { admissions++; }

                totalAdmissions += admissions;
                String region = admitEvent.getRegionID();
                if (region != null) {
                    counts.put(region, counts.getOrDefault(region, 0L) + admissions);
                }
            }

            out.collect(new NetworkAdmissionStatistics(context.window().getStart(),context.window().getEnd(),totalAdmissions,counts));
        }
    }
}
