package org.muralis.flink.launcher;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.muralis.flink.data.model.AdmitEvent;
import org.muralis.flink.data.model.BedOccupancyAlert;
import org.muralis.flink.data.model.DischargeEvent;
import org.muralis.flink.data.source.hospital.HospitalAdmissionAnalytics;
import org.muralis.flink.data.source.hospital.HospitalBedOccupancyMonitor;
import org.muralis.flink.data.source.hospital.PatientAdmissionSource;
import org.muralis.flink.data.source.hospital.PatientLifecycleSimulator;
import org.muralis.flink.data.source.hospital.RegionLengthOfStaySampler;

/**
 * LESSON 2B - Admissions <b>and</b> discharges: tracking live bed occupancy per hospital.
 *
 * <p>Demonstrates:
 * <ul>
 *   <li>Per-key event-time timers ({@link PatientLifecycleSimulator}, keyed by {@code patientID})
 *       that discharge each real, previously-admitted patient after a region-dependent length of stay</li>
 *   <li>Emitting discharges on a side output ({@link PatientLifecycleSimulator#DISCHARGE_TAG})</li>
 *   <li>{@code connect(...)} + {@code keyBy(...)} + {@code KeyedCoProcessFunction}
 *       ({@link HospitalBedOccupancyMonitor}) to combine admissions and discharges into occupancy</li>
 *   <li>Alerting on every status transition (NORMAL / HIGH / FULL) with hysteresis</li>
 *   <li>Checkpointing plus stable operator UIDs, so pending discharges (timers + state) survive failures</li>
 * </ul>
 *
 * <p>Region "S" has a much longer mean length of stay, so by Little's law
 * ({@code occupancy = arrivalRate × meanLengthOfStay}) its hospitals run out of beds while the
 * other regions settle below the alert threshold. See {@code docs/discharge_event_plan.md}.
 *
 * <p>Run with:
 * <pre>
 *   ./run.sh 2B
 * </pre>
 *
 * <p>At the defaults, region S takes about 6.5 minutes to reach HIGH: its 100-bed capacity
 * ({@link org.muralis.flink.data.source.hospital.HospitalCapacityRegistry}) is deliberately small,
 * which is what makes it reach the alert thresholds quickly without touching admission rate or
 * length of stay at all. To see it sooner or later, shrink or grow that capacity, or shorten or
 * lengthen the length-of-stay ranges in {@link RegionLengthOfStaySampler}, directly.
 *
 * <p>Note: {@code print()} is not a transactional sink, so after a restore from a checkpoint some
 * alerts can be printed twice. The state itself (occupancy, pending discharges) is exactly-once.
 */
public class Lesson2B {

    public static final long CHECKPOINT_INTERVAL_MS = 10_000L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);

        double admissionsPerSecond = resolveDouble("ADMISSION_RATE_PER_SECOND", PatientAdmissionSource.DEFAULT_RATE_PER_SECOND);
        System.out.printf("Starting Hospital Bed Occupancy Monitor at %.1f admissions/sec...%n", admissionsPerSecond);

        // 1. Simulated patient admissions across 4 regions, with event-time timestamps (admitTime).
        DataStream<AdmitEvent> admissionsStream = PatientAdmissionSource.admissions(env, admissionsPerSecond);

        // 2. Schedule every admitted patient's discharge. Admissions pass through on the main
        //    output; discharges come out on a side output when their event-time timer fires.
        SingleOutputStreamOperator<AdmitEvent> admits = HospitalAdmissionAnalytics.attachPatientLifecycleSimulator(
                admissionsStream, RegionLengthOfStaySampler.defaults());
        DataStream<DischargeEvent> discharges = admits.getSideOutput(PatientLifecycleSimulator.DISCHARGE_TAG);

        // 3. Combine both streams per hospital into live bed occupancy, alerting on every status change.
        DataStream<BedOccupancyAlert> occupancyAlerts = admits
                .connect(discharges)
                .keyBy(AdmitEvent::getHospitalID, DischargeEvent::getHospitalID)
                .process(new HospitalBedOccupancyMonitor())
                .uid("bed-occupancy-monitor")
                .name("bed-occupancy-monitor");

        occupancyAlerts.print("BED-OCCUPANCY").name("print-bed-occupancy-alerts");

        env.execute("Hospital Bed Occupancy with Admissions and Discharges");
    }

    private static double resolveDouble(String envVar, double defaultValue) {
        String override = System.getenv(envVar);
        if (override != null && !override.isBlank()) {
            return Double.parseDouble(override.trim());
        }
        return defaultValue;
    }
}
