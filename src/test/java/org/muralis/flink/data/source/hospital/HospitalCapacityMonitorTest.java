package org.muralis.flink.data.source.hospital;

import java.util.Queue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muralis.flink.data.model.AdmitEvent;
import org.muralis.flink.data.model.CapacityAlert;
import org.muralis.flink.data.model.Patient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HospitalCapacityMonitor} using Flink's real (non-mocked)
 * {@link ProcessFunctionTestHarnesses}, which drives an actual {@link KeyedProcessOperator}
 * instance against the function under test.
 */
class HospitalCapacityMonitorTest {

    /** Region "S" hospitals have a 500-bed max capacity; 90% of that is 450. */
    private static final String HOSPITAL_S1 = "HS1";
    private static final String REGION_S = "S";

    private KeyedOneInputStreamOperatorTestHarness<String, AdmitEvent, AdmitEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        KeyedProcessFunction<String, AdmitEvent, AdmitEvent> function = new HospitalCapacityMonitor();
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                AdmitEvent::getHospitalID,
                TypeInformation.of(String.class));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void forwardsEveryInputRecordOnTheMainOutput() throws Exception {
        AdmitEvent admitEvent = admissionFor(HOSPITAL_S1);

        harness.processElement(admitEvent, 1L);

        Queue<Object> output = harness.getOutput();
        assertEquals(1, output.size());
        assertEquals(admitEvent, ((StreamRecord<?>) output.poll()).getValue());
    }

    @Test
    void doesNotAlertBelowNinetyPercentCapacity() throws Exception {
        // 440 of 500 beds = 88%, below the 90% threshold.
        processAdmissions(HOSPITAL_S1, 440, 1L);

        assertNoAlertsRaised();
    }

    @Test
    void alertsExactlyOnceWhenCrossingNinetyPercentCapacity() throws Exception {
        // 449 of 500 beds = 89.8%: still below threshold.
        processAdmissions(HOSPITAL_S1, 449, 1L);
        assertNoAlertsRaised();

        // One more admission crosses 90% (450/500 = 90.0%): alert should fire now.
        harness.processElement(admissionFor(HOSPITAL_S1), 2L);

        Queue<StreamRecord<CapacityAlert>> alerts = harness.getSideOutput(HospitalCapacityMonitor.CAPACITY_ALERT_TAG);
        assertEquals(1, alerts.size());

        CapacityAlert alert = alerts.poll().getValue();
        assertEquals(HOSPITAL_S1, alert.getHospitalID());
        assertEquals(REGION_S, alert.getRegionID());
        assertEquals(450L, alert.getCurrentOccupancy());
        assertEquals(500, alert.getMaxCapacity());
        assertEquals(90.0, alert.getOccupancyPercentage(), 0.001);

        // Further admissions must not raise a second alert for the same hospital.
        processAdmissions(HOSPITAL_S1, 10, 3L);
        assertTrue(alerts.isEmpty(), "no additional alerts should have been queued");
    }

    /** The harness returns {@code null} for a side-output tag until something has been emitted to it. */
    private void assertNoAlertsRaised() {
        Queue<StreamRecord<CapacityAlert>> alerts = harness.getSideOutput(HospitalCapacityMonitor.CAPACITY_ALERT_TAG);
        assertTrue(alerts == null || alerts.isEmpty());
    }

    /** Sends {@code admissionCount} individual {@link AdmitEvent} records for {@code hospitalID}. */
    private void processAdmissions(String hospitalID, int admissionCount, long timestamp) throws Exception {
        for (int i = 0; i < admissionCount; i++) {
            harness.processElement(admissionFor(hospitalID), timestamp);
        }
    }

    /** Builds a single {@link AdmitEvent} for {@code hospitalID}. */
    private static AdmitEvent admissionFor(String hospitalID) {
        return new AdmitEvent(new Patient("P1", "F", 70), hospitalID, System.currentTimeMillis());
    }
}
