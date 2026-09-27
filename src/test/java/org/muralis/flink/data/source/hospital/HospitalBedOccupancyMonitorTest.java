package org.muralis.flink.data.source.hospital;

import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muralis.flink.data.model.AdmitEvent;
import org.muralis.flink.data.model.BedOccupancyAlert;
import org.muralis.flink.data.model.BedOccupancyStatus;
import org.muralis.flink.data.model.DischargeEvent;
import org.muralis.flink.data.model.Patient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.muralis.flink.data.model.BedOccupancyStatus.FULL;
import static org.muralis.flink.data.model.BedOccupancyStatus.HIGH;
import static org.muralis.flink.data.model.BedOccupancyStatus.NORMAL;

/**
 * Tests {@link HospitalBedOccupancyMonitor} using Flink's real (non-mocked)
 * {@link ProcessFunctionTestHarnesses}.
 *
 * <p>Region "S" hospitals have 100 beds, so the hysteresis boundaries are:
 * HIGH at 90, back to NORMAL below 85, FULL at 100, back to HIGH below 95.
 */
class HospitalBedOccupancyMonitorTest {

    private static final String HOSPITAL_S1 = "HS1";
    private static final String HOSPITAL_S2 = "HS2";

    private KeyedTwoInputStreamOperatorTestHarness<String, AdmitEvent, DischargeEvent, BedOccupancyAlert> harness;
    private long time;

    @BeforeEach
    void setUp() throws Exception {
        // The factory already opens the harness.
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new HospitalBedOccupancyMonitor(),
                AdmitEvent::getHospitalID,
                DischargeEvent::getHospitalID,
                Types.STRING);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void walksThroughEveryTransitionInBothDirections() throws Exception {
        admit(HOSPITAL_S1, 89);
        assertNoAlerts();

        admit(HOSPITAL_S1, 1); // 90/100 = 90%
        assertAlert(NORMAL, HIGH, 90);

        admit(HOSPITAL_S1, 9); // 99/100
        assertNoAlerts();
        admit(HOSPITAL_S1, 1); // 100/100 = 100%
        assertAlert(HIGH, FULL, 100);

        discharge(HOSPITAL_S1, 5); // 95/100 = 95%: still FULL
        assertNoAlerts();
        discharge(HOSPITAL_S1, 1); // 94/100 = 94%
        assertAlert(FULL, HIGH, 94);

        discharge(HOSPITAL_S1, 9); // 85/100 = 85%: still HIGH
        assertNoAlerts();
        discharge(HOSPITAL_S1, 1); // 84/100 = 84%
        assertAlert(HIGH, NORMAL, 84);
    }

    @Test
    void doesNotReAlertWhileOscillatingInsideTheHysteresisBand() throws Exception {
        admit(HOSPITAL_S1, 90);
        assertAlert(NORMAL, HIGH, 90);

        // Bounce between 85% and 89%: stays HIGH, no alert storm.
        discharge(HOSPITAL_S1, 5); // 85
        for (int i = 0; i < 5; i++) {
            admit(HOSPITAL_S1, 4);    // 89
            discharge(HOSPITAL_S1, 4); // 85
        }
        admit(HOSPITAL_S1, 4); // 89
        assertNoAlerts();

        // Crossing back up to 90% from HIGH is not a transition either.
        admit(HOSPITAL_S1, 1);
        assertNoAlerts();
    }

    @Test
    void alertCarriesHospitalRegionCapacityAndEventTime() throws Exception {
        admit(HOSPITAL_S1, 89);
        harness.processElement1(admission(HOSPITAL_S1), 42_000L);

        List<BedOccupancyAlert> alerts = drainAlerts();
        assertEquals(1, alerts.size());
        BedOccupancyAlert alert = alerts.get(0);
        assertEquals(HOSPITAL_S1, alert.getHospitalID());
        assertEquals("S", alert.getRegionID());
        assertEquals(100, alert.getMaxCapacity());
        assertEquals(90.0, alert.getOccupancyPercentage(), 0.001);
        assertEquals(42_000L, alert.getAlertTime());
    }

    @Test
    void dischargeFromAnEmptyHospitalIsFlooredAtZero() throws Exception {
        discharge(HOSPITAL_S1, 3);
        assertNoAlerts();

        // Had occupancy gone to -3, 93 admissions would be needed to reach HIGH.
        admit(HOSPITAL_S1, 90);
        assertAlert(NORMAL, HIGH, 90);
    }

    @Test
    void unknownHospitalsAreIgnored() throws Exception {
        admit("UNKNOWN", 1_000);
        discharge("UNKNOWN", 2_000);

        assertNoAlerts();
        assertEquals(0, harness.numKeyedStateEntries());
    }

    @Test
    void occupancyIsTrackedPerHospital() throws Exception {
        admit(HOSPITAL_S1, 90);
        admit(HOSPITAL_S2, 89);

        List<BedOccupancyAlert> alerts = drainAlerts();
        assertEquals(1, alerts.size());
        assertEquals(HOSPITAL_S1, alerts.get(0).getHospitalID());

        admit(HOSPITAL_S2, 1);
        alerts = drainAlerts();
        assertEquals(1, alerts.size());
        assertEquals(HOSPITAL_S2, alerts.get(0).getHospitalID());
        assertEquals(90, alerts.get(0).getOccupied());
    }

    private void admit(String hospitalID, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            harness.processElement1(admission(hospitalID), ++time);
        }
    }

    private void discharge(String hospitalID, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            ++time;
            harness.processElement2(new DischargeEvent("P1", hospitalID, 0L, time), time);
        }
    }

    private void assertAlert(BedOccupancyStatus from, BedOccupancyStatus to, int occupied) {
        List<BedOccupancyAlert> alerts = drainAlerts();
        assertEquals(1, alerts.size(), "expected exactly one alert, got " + alerts);
        BedOccupancyAlert alert = alerts.get(0);
        assertEquals(from, alert.getPreviousStatus());
        assertEquals(to, alert.getStatus());
        assertEquals(occupied, alert.getOccupied());
    }

    private void assertNoAlerts() {
        List<BedOccupancyAlert> alerts = drainAlerts();
        assertTrue(alerts.isEmpty(), "expected no alerts, got " + alerts);
    }

    private List<BedOccupancyAlert> drainAlerts() {
        List<BedOccupancyAlert> alerts = harness.extractOutputValues();
        harness.getOutput().clear();
        return alerts;
    }

    private static AdmitEvent admission(String hospitalID) {
        return new AdmitEvent(new Patient("P1", "F", 70), hospitalID, 0L);
    }
}
