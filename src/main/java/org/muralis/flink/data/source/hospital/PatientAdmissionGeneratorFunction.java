package org.muralis.flink.data.source.hospital;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.muralis.flink.data.model.AdmitEvent;
import org.muralis.flink.data.model.Patient;

import java.io.Serial;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Flink {@link GeneratorFunction} implementation that simulates patient admissions across
 * a multi-region hospital network.
 *
 * <p>Hospital network distribution:
 * <ul>
 *   <li>Region <b>NE</b>: HNE1, HNE2, HNE3, HNE4</li>
 *   <li>Region <b>MW</b>: HMW1, HMW2, HMW3</li>
 *   <li>Region <b>W</b>:  HW1, HW2, HW3, HW4, HW5</li>
 *   <li>Region <b>S</b>:  HS1, HS2</li>
 * </ul>
 *
 * <p>Patient demographic profiles are distributed unevenly:
 * <ul>
 *   <li>Genders: Female (~52%), Male (~44%), Other (~4%)</li>
 *   <li>Age groups: Pediatric (0-17, ~15%), Young Adult (18-35, ~20%),
 *       Middle-Aged (36-64, ~25%), Senior/Geriatric (65-95, ~40%)</li>
 * </ul>
 *
 * <p>Emits one {@link AdmitEvent} per invocation, matching how admissions occur in the real
 * world: one discrete event per admission, not a batched snapshot.
 */
public class PatientAdmissionGeneratorFunction implements GeneratorFunction<Long, AdmitEvent> {

    @Serial
    private static final long serialVersionUID = 1L;

    public static final String REGION_NE = "NE";
    public static final String REGION_MW = "MW";
    public static final String REGION_W = "W";
    public static final String REGION_S = "S";

    public static final Map<String, List<String>> REGION_HOSPITALS;

    static {
        Map<String, List<String>> map = new LinkedHashMap<>();
        map.put(REGION_NE, List.of("HNE1", "HNE2", "HNE3", "HNE4"));
        map.put(REGION_MW, List.of("HMW1", "HMW2", "HMW3"));
        map.put(REGION_W, List.of("HW1", "HW2", "HW3", "HW4", "HW5"));
        map.put(REGION_S, List.of("HS1", "HS2"));
        REGION_HOSPITALS = Collections.unmodifiableMap(map);
    }

    private static final List<String> REGIONS = List.of(REGION_NE, REGION_MW, REGION_W, REGION_S);

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        // Called once upon task initialization in the Flink source reader
    }

    @Override
    public void close() throws Exception {
        // Called once upon task tear-down
    }

    @Override
    public AdmitEvent map(Long index) throws Exception {
        Random random = ThreadLocalRandom.current();

        // 1. Randomly select a region
        String regionID = REGIONS.get(random.nextInt(REGIONS.size()));

        // 2. Randomly select a hospital within the chosen region
        List<String> hospitals = REGION_HOSPITALS.get(regionID);
        String hospitalID = hospitals.get(random.nextInt(hospitals.size()));

        // 3. Generate uneven patient profile
        Patient patient = generatePatient(random, index);

        // 4. Create admission event with current epoch timestamp
        long admitTime = System.currentTimeMillis();
        return new AdmitEvent(patient, hospitalID, admitTime);
    }

    /**
     * Generates a realistic patient profile with uneven gender and age distributions.
     */
    public Patient generatePatient(Random random, Long index) {
        long idNum = index != null ? (100000L + (index % 900000L)) : (100000L + random.nextInt(900000));
        String patientID = String.format("PAT-%06d", idNum);
        String gender = generateUnevenGender(random);
        int age = generateUnevenAge(random);

        return new Patient(patientID, gender, age);
    }

    /**
     * Uneven gender distribution:
     * - Female: 52%
     * - Male: 44%
     * - Other: 4%
     */
    public String generateUnevenGender(Random random) {
        int roll = random.nextInt(100);
        if (roll < 52) {
            return "Female";
        } else if (roll < 96) {
            return "Male";
        } else {
            return "Other";
        }
    }

    /**
     * Uneven age group distribution (reflecting emergency room demographics):
     * - Pediatric (0 - 17): 15%
     * - Young Adult (18 - 35): 20%
     * - Middle-Aged (36 - 64): 25%
     * - Senior / Geriatric (65 - 95): 40%
     */
    public int generateUnevenAge(Random random) {
        int roll = random.nextInt(100);
        if (roll < 15) {
            return random.nextInt(18); // 0 - 17
        } else if (roll < 35) {
            return 18 + random.nextInt(18); // 18 - 35
        } else if (roll < 60) {
            return 36 + random.nextInt(29); // 36 - 64
        } else {
            return 65 + random.nextInt(31); // 65 - 95
        }
    }
}
