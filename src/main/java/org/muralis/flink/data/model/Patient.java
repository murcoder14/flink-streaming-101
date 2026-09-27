package org.muralis.flink.data.model;

import java.io.Serial;
import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Represents a patient with identifier, gender, and age.
 * Follows Flink POJO conventions for serialization and state management.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Patient implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String patientID;
    private String gender;
    private int age;

    @Override
    public String toString() {
        return "Patient{" +
                "patientID='" + patientID + '\'' +
                ", gender='" + gender + '\'' +
                ", age=" + age +
                '}';
    }
}
