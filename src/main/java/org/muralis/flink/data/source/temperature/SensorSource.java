package org.muralis.flink.data.source.temperature;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.muralis.flink.data.model.SensorReading;

import java.util.Random;

/**
 * Creates the never-ending stream of fake sensor readings that all lessons use, so that you do not need Kafka or any
 * other external system to try things out. Nothing in here is a "lesson" - skim it once and move on.
 *
 * What the stream looks like (2 readings per second):
 *   - three sensors, "sensor-1", "sensor-2", "sensor-3", take turns; sensor-1 is the coolest, sensor-3 the warmest
 *   - every 17th reading is a temperature spike (+8 degrees)
 *   - every 23rd reading is faulty (temperature = -999)
 *   - after 40 readings (~20 s) sensor-3 stops reporting - useful for the timer lesson
 */
public final class SensorSource {

    /** A reading below this value is a faulty measurement. */
    public static final double FAULTY = -999;

    private SensorSource() { }

    /** The default stream: 2 readings per second. */
    public static DataStream<SensorReading> readings(StreamExecutionEnvironment env) {
        return readings(env, 2);
    }

    public static DataStream<SensorReading> readings(StreamExecutionEnvironment env, double readingsPerSecond) {
        readingsPerSecond = rateOverride(readingsPerSecond);

        // A "source" is where a stream starts. DataGeneratorSource calls our function with 0, 1, 2, 3, ... and
        // sends whatever it returns into the stream. perSecond(...) slows it down, by default to 2 records per second.
        DataGeneratorSource<SensorReading> source = new DataGeneratorSource<>(
                new ReadingGenerator(),
                Long.MAX_VALUE,                      // "never stop"
                RateLimiterStrategy.perSecond(readingsPerSecond),
                TypeInformation.of(SensorReading.class));

        // Watermarks tell Flink how far event time has progressed ("no reading older than timestamp X will arrive anymore").
        // Only the timer lesson needs them. Our readings are created in timestamp order, so the simplest strategy,
        // "monotonously increasing timestamps", is enough. The assigner says where the event time is stored.
        WatermarkStrategy<SensorReading> watermarks = WatermarkStrategy
                .<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner((reading, previousTimestamp) -> reading.timestamp);

        return env.fromSource(source, watermarks, "temperature-readings")
                .setParallelism(1); // one generator, so that the readings stay in order
    }

    /**
     * Load knob for experiments. By default, the lessons run at 2 readings per second, which is so little that the
     * Flink UI shows an idle job (an empty flame graph, no backpressure). Setting the environment variable
     * SENSOR_RATE=<readings per second> before starting a lesson overrides the rate of ALL lessons, e.g.
     * `SENSOR_RATE=5000 ./docker-run.sh 1D`. Not set (or empty) means: use the lesson's own rate.
     */
    private static double rateOverride(double requested) {
        String override = System.getenv("SENSOR_RATE");
        return override == null || override.isBlank() ? requested : Double.parseDouble(override.trim());
    }

    /** Turns the running number 0, 1, 2, ... into a reading. Deterministic apart from the timestamp. */
    static class ReadingGenerator implements GeneratorFunction<Long, SensorReading> {
        @Override
        public SensorReading map(Long index) {
            int sensorNumber = index < 40
                    ? 1 + (int) (index % 3)   // sensors 1, 2, 3, 1, 2, 3, ...
                    : 1 + (int) (index % 2);  // later only 1, 2, 1, 2, ...: sensor-3 went quiet
            double temperature = 16 + 4 * sensorNumber + new Random(index).nextGaussian(); // 20, 24, 28 (+/- noise)
            if (index % 17 == 0) {
                temperature += 8;
            }
            if (index % 23 == 0) {
                temperature = FAULTY;
            }
            return new SensorReading("sensor-" + sensorNumber, System.currentTimeMillis(),
                    Math.round(temperature * 10) / 10.0);
        }
    }
}
