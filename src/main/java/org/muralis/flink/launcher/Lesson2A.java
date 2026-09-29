package org.muralis.flink.launcher;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.muralis.flink.data.model.AdmitEvent;
import org.muralis.flink.data.model.CapacityAlert;
import org.muralis.flink.data.model.NetworkAdmissionStatistics;
import org.muralis.flink.data.model.RegionalAdmissionCount;
import org.muralis.flink.data.source.hospital.HospitalAdmissionAnalytics;
import org.muralis.flink.data.source.hospital.HospitalCapacityMonitor;
import org.muralis.flink.data.source.hospital.PatientAdmissionSource;

import java.time.Duration;

/**
 * LESSON 2A - Measuring Hospital Admissions across all 4 regions in a 2-minute interval.
 *
 * <p>Demonstrates:
 * <ul>
 *   <li>Simulating patient admissions across 4 hospital regions (NE, MW, W, S) using FLIP-27 Source</li>
 *   <li>Pacing generation with RateLimiterStrategy.perSecond(2)</li>
 *   <li>Measuring total admissions across all 4 regions in a 2-minute interval</li>
 *   <li>Measuring per-region admission counts using keyBy + Tumbling Window</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   mvn -q -B -Plocal compile exec:exec -Dexec.executable=java -Dexec.args="-cp %classpath org.muralis.flink.launcher.Lesson2A"
 * </pre>
 *
 * <p>Note: For rapid local testing without waiting a full 2 minutes, set the environment variable:
 * <pre>
 *   ADMISSION_WINDOW_SECONDS=10 mvn -q -B -Plocal compile exec:exec -Dexec.executable=java -Dexec.args="-cp %classpath org.muralis.flink.launcher.Lesson2A"
 * </pre>
 */
public class Lesson2A {

    public static final Duration DEFAULT_WINDOW_INTERVAL = Duration.ofMinutes(2);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Duration windowInterval = resolveWindowInterval();
        System.out.printf("Starting Hospital Admission Monitor with a %s window interval...%n", windowInterval);

        // 1. Create simulated patient admissions stream across 4 regions at 2 admissions/sec
        DataStream<AdmitEvent> admissionsStream = PatientAdmissionSource.admissions(env);

        // 2. Measure admissions across all 4 regions combined in a 2-minute interval
        //DataStream<NetworkAdmissionStatistics> networkSummary =  HospitalAdmissionAnalytics.measureAcrossAllRegions(admissionsStream, windowInterval);
        //networkSummary.print("NETWORK-ALL-REGIONS").name("print-all-regions-admissions");

        // 3. Measure admissions grouped by region in the same 2-minute interval
        //DataStream<RegionalAdmissionCount> regionalCounts = HospitalAdmissionAnalytics.measureByRegion(admissionsStream, windowInterval);
        //regionalCounts.print("BY-REGION").name("print-regional-admissions");

        // 4. Measure senior citizen admissions in region "NE" in the same 2-minute interval
        DataStream<RegionalAdmissionCount> regionalCountsNE = HospitalAdmissionAnalytics.measureByRegion(admissionsStream, "NE", windowInterval);
        regionalCountsNE.print("BY-REGION-NE").name("print-senior-citizen-only-admissions-ne");

        // 5. Monitor per-hospital bed capacity and raise an alert (side output) once cumulative
        //    admissions reach 90% of a hospital's maximum bed capacity.
        SingleOutputStreamOperator<AdmitEvent> monitoredAdmissions = HospitalAdmissionAnalytics.attachCapacityMonitor(admissionsStream);
        DataStream<CapacityAlert> capacityAlerts = monitoredAdmissions.getSideOutput(HospitalCapacityMonitor.CAPACITY_ALERT_TAG);
        capacityAlerts.print("CAPACITY-ALERT").name("print-capacity-alerts");

        env.execute("Hospital Patient Admissions 2-Minute Window Measurement");
    }

    private static Duration resolveWindowInterval() {
        String override = System.getenv("ADMISSION_WINDOW_SECONDS");
        if (override != null && !override.isBlank()) {
            return Duration.ofSeconds(Long.parseLong(override.trim()));
        }
        return DEFAULT_WINDOW_INTERVAL;
    }
}
