package org.muralis.flink.data.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.muralis.flink.data.source.hospital.HospitalCapacityRegistry;

/**
 * Represents a single patient admission event: the patient, the admitting hospital, and the
 * admit time. This is the unit streamed by the patient-admissions source: one record per
 * real-world admission, matching how admissions actually occur (e.g. HL7 ADT-style feeds).
 *
 * <p>{@code regionID} is intentionally NOT stored on this event. A hospital's region is
 * slowly-changing reference data, not a per-event fact, so it is resolved on demand via
 * {@link HospitalCapacityRegistry#regionForHospital(String)} rather than duplicated on every
 * event.
 *
 * <p>Follows Flink POJO conventions for serialization and state management.
 */
@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class AdmitEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Patient patient;
    private String hospitalID;
    /** Event time: epoch milliseconds when the patient was admitted. */
    private long admitTime;

    public AdmitEvent(Patient patient, String hospitalID, long admitTime) {
        this.patient = patient;
        this.hospitalID = hospitalID;
        this.admitTime = admitTime;
    }

    public AdmitEvent(Patient patient, String hospitalID, Instant admitTime) {
        this.patient = patient;
        this.hospitalID = hospitalID;
        this.admitTime = admitTime != null ? admitTime.toEpochMilli() : 0L;
    }

    /** Derived from {@link #hospitalID} via {@link HospitalCapacityRegistry}; not stored. */
    public String getRegionID() {
        return HospitalCapacityRegistry.regionForHospital(hospitalID);
    }

    public Instant getAdmitTimeAsInstant() {
        return Instant.ofEpochMilli(admitTime);
    }

    public void setAdmitTime(Instant admitTime) {
        this.admitTime = admitTime != null ? admitTime.toEpochMilli() : 0L;
    }

    @Override
    public String toString() {
        return "AdmitEvent{" +
                "patient=" + patient +
                ", hospitalID='" + hospitalID + '\'' +
                ", admitTime=" + admitTime +
                '}';
    }
}
