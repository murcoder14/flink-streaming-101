package org.muralis.flink.data.source.hospital;

import java.util.List;
import java.util.Queue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muralis.flink.data.model.AdmitEvent;
import org.muralis.flink.data.model.DischargeEvent;
import org.muralis.flink.data.model.Patient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link PatientLifecycleSimulator} using Flink's real (non-mocked)
 * {@link ProcessFunctionTestHarnesses}. Event time is advanced with watermarks, so every
 * discharge timestamp is deterministic.
 */
class PatientLifecycleSimulatorTest {

    private static final long LOS = 1_000L;
    private static final long ADMIT_TS = 10_000L;
    private static final String HOSPITAL_S1 = "HS1";

    private static final KeySelector<AdmitEvent, String> BY_PATIENT = admitEvent -> admitEvent.getPatient().getPatientID();

    private KeyedOneInputStreamOperatorTestHarness<String, AdmitEvent, AdmitEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        // The factory already opens the harness.
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new PatientLifecycleSimulator(new FixedLengthOfStaySampler(LOS)),
                BY_PATIENT,
                Types.STRING);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void dischargesTheSamePatientExactlyAtAdmitTimePlusLengthOfStay() throws Exception {
        harness.processElement(admission("PAT-1", HOSPITAL_S1, ADMIT_TS), ADMIT_TS);
        assertEquals(1, harness.numEventTimeTimers());

        harness.processWatermark(ADMIT_TS + LOS - 1);
        assertNoDischarges();

        harness.processWatermark(ADMIT_TS + LOS);
        Queue<StreamRecord<DischargeEvent>> discharges = harness.getSideOutput(PatientLifecycleSimulator.DISCHARGE_TAG);
        assertEquals(1, discharges.size());

        StreamRecord<DischargeEvent> record = discharges.poll();
        assertEquals(ADMIT_TS + LOS, record.getTimestamp());
        assertEquals(new DischargeEvent("PAT-1", HOSPITAL_S1, ADMIT_TS, ADMIT_TS + LOS), record.getValue());
        assertEquals(0, harness.numEventTimeTimers());
        assertEquals(0, harness.numKeyedStateEntries(), "state must be cleared once the patient is discharged");
    }

    @Test
    void forwardsEveryAdmissionUnchangedOnTheMainOutput() throws Exception {
        AdmitEvent first = admission("PAT-1", HOSPITAL_S1, ADMIT_TS);
        AdmitEvent second = admission("PAT-2", "HNE1", ADMIT_TS + 5);

        harness.processElement(first, ADMIT_TS);
        harness.processElement(second, ADMIT_TS + 5);
        harness.processWatermark(ADMIT_TS + 10 * LOS);

        assertEquals(List.of(first, second), harness.extractOutputValues());
    }

    @Test
    void keepsOneTimerPerPatientEvenWhenDischargesCollide() throws Exception {
        // Same hospital, same admit millisecond, same LOS: keyed by patient, both discharges survive.
        harness.processElement(admission("PAT-1", HOSPITAL_S1, ADMIT_TS), ADMIT_TS);
        harness.processElement(admission("PAT-2", HOSPITAL_S1, ADMIT_TS), ADMIT_TS);
        assertEquals(2, harness.numEventTimeTimers());

        harness.processWatermark(ADMIT_TS + LOS);

        assertEquals(2, harness.getSideOutput(PatientLifecycleSimulator.DISCHARGE_TAG).size());
    }

    @Test
    void reAdmissionWhileStillAdmittedKeepsTheExistingStay() throws Exception {
        harness.processElement(admission("PAT-1", HOSPITAL_S1, ADMIT_TS), ADMIT_TS);
        AdmitEvent readmission = admission("PAT-1", "HNE1", ADMIT_TS + 500);
        harness.processElement(readmission, ADMIT_TS + 500);

        assertEquals(1, harness.numEventTimeTimers());
        assertTrue(harness.extractOutputValues().contains(readmission), "the re-admission is still forwarded");

        harness.processWatermark(ADMIT_TS + 500 + LOS);
        Queue<StreamRecord<DischargeEvent>> discharges = harness.getSideOutput(PatientLifecycleSimulator.DISCHARGE_TAG);
        assertEquals(1, discharges.size());
        DischargeEvent discharge = discharges.poll().getValue();
        assertEquals(HOSPITAL_S1, discharge.getHospitalID());
        assertEquals(ADMIT_TS + LOS, discharge.getDischargeTime());
    }

    @Test
    void pendingDischargesSurviveSnapshotAndRestore() throws Exception {
        harness.processElement(admission("PAT-1", HOSPITAL_S1, ADMIT_TS), ADMIT_TS);
        OperatorSubtaskState snapshot = harness.snapshot(1L, ADMIT_TS);
        harness.close();

        // Simulated failover: a fresh operator instance restored from the checkpoint.
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new PatientLifecycleSimulator(new FixedLengthOfStaySampler(LOS))),
                BY_PATIENT,
                Types.STRING);
        harness.initializeState(snapshot);
        harness.open();
        assertEquals(1, harness.numEventTimeTimers());

        harness.processWatermark(ADMIT_TS + LOS);

        Queue<StreamRecord<DischargeEvent>> discharges = harness.getSideOutput(PatientLifecycleSimulator.DISCHARGE_TAG);
        assertEquals(1, discharges.size());
        assertEquals("PAT-1", discharges.poll().getValue().getPatientID());
    }

    @Test
    void recordsAreFlinkPojosNotKryoGenericTypes() {
        assertInstanceOf(PojoTypeInfo.class, TypeInformation.of(DischargeEvent.class));
        assertInstanceOf(PojoTypeInfo.class, TypeInformation.of(org.muralis.flink.data.model.BedOccupancyAlert.class));
    }

    /** The harness returns {@code null} for a side-output tag until something has been emitted to it. */
    private void assertNoDischarges() {
        Queue<StreamRecord<DischargeEvent>> discharges = harness.getSideOutput(PatientLifecycleSimulator.DISCHARGE_TAG);
        assertTrue(discharges == null || discharges.isEmpty());
    }

    private static AdmitEvent admission(String patientID, String hospitalID, long admitTime) {
        return new AdmitEvent(new Patient(patientID, "F", 70), hospitalID, admitTime);
    }
}
