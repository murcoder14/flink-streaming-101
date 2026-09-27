package org.muralis.flink.data.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.muralis.flink.data.source.hospital.HospitalCapacityRegistry;

/**
 * Represents a single patient discharge: the (real, previously-admitted) patient, the hospital
 * that discharges them, when they were admitted and when they were discharged.
 *
 * <p>{@code patientID} is always copied from a prior {@link AdmitEvent}; it is never synthesised.
 * {@code dischargeTime} is the event-time timer timestamp at which the discharge fired, not the
 * wall-clock time at which the record happened to be processed.
 *
 * <p>Like {@link AdmitEvent}, {@code regionID} is not stored; it is resolved on demand via
 * {@link HospitalCapacityRegistry#regionForHospital(String)}.
 *
 * <p>Follows Flink POJO conventions for serialization and state management.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class DischargeEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String patientID;
    private String hospitalID;
    /** Event time: epoch milliseconds when the patient was admitted. */
    private long admitTime;
    /** Event time: epoch milliseconds when the patient was discharged. */
    private long dischargeTime;

    /** Derived from {@link #hospitalID} via {@link HospitalCapacityRegistry}; not stored. */
    public String getRegionID() {
        return HospitalCapacityRegistry.regionForHospital(hospitalID);
    }

    public Instant getDischargeTimeAsInstant() {
        return Instant.ofEpochMilli(dischargeTime);
    }

    @Override
    public String toString() {
        return "DischargeEvent{" +
                "patientID='" + patientID + '\'' +
                ", hospitalID='" + hospitalID + '\'' +
                ", admitTime=" + admitTime +
                ", dischargeTime=" + dischargeTime +
                '}';
    }
}
