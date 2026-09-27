package org.muralis.flink.launcher;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.muralis.flink.data.model.SensorReading;
import org.muralis.flink.data.source.temperature.SensorSource;

/**
 * LESSON 1 - Basic operators: filter, map, flatMap, keyBy
 * A Flink program is a chain of steps ("operators"). Each operator takes a stream in and returns a new stream:
 *
 *   readings --filter--> valid --map--> MAP
 *                          |--flatMap--> FLATMAP
 *                          |--keyBy----> KEYBY
 *
 *   filter   keeps a record or drops it              (1 record in -> 0 or 1 out, same type)
 *   map      transforms every record                 (1 record in -> exactly 1 out, any type)
 *   flatMap  like map, but may emit 0, 1 or many     (1 record in -> 0..n out)
 *   keyBy    groups records by a key, so that all records with the same key are handled by the same parallel
 *            worker. Almost everything "stateful" (lessons 2-4) starts with keyBy.
 *
 * Run it:  ./run.sh 1        (stop with Ctrl-C)
 */
public class Lesson1D {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parallelism = how many parallel workers ("subtasks") run each operator. We use 2 here so that you can SEE
        // what keyBy does. Every printed line starts with NAME:<worker number>>, e.g. "KEYBY:2> ...".
        env.setParallelism(2);

        DataStream<SensorReading> readings = SensorSource.readings(env);

        // ---- filter ---------------------------------------------------------------------------------------------
        // The function returns true for records that should stay. Faulty sensors report -999: throw those away.
        //
        // .name(...) gives the operator a label. It changes nothing about what the job does, but it is what the Flink
        // Web UI shows in the job graph, in metrics and in flame graphs - without it you would only see "Filter" and
        // "Map". Every operator (and every sink) accepts .name(...); give them names that point back to this code.
        DataStream<SensorReading> valid = readings
                .filter(reading -> reading.temperature != SensorSource.FAULTY)
                .name("filter-faulty-readings");

        // ---- map ------------------------------------------------------------------------------------------------
        // Exactly one output per input. Here we also change the type: SensorReading -> String (Celsius to Fahrenheit).
        DataStream<String> inFahrenheit = valid
                .map(reading -> reading.sensorId + " is at " + Math.round((reading.temperature * 9 / 5 + 32) * 10) / 10.0 + " F")
                .name("map-to-fahrenheit");
        inFahrenheit.print("MAP").name("print-fahrenheit");   // print(...) is a sink; sinks can be named too

        // ---- flatMap --------------------------------------------------------------------------------------------
        // Zero, one or two outputs per input, decided by the function (see WarningFlatMap below).
        DataStream<String> warnings = valid
                .flatMap(new WarningFlatMap())
                .name("flatmap-warnings");
        warnings.print("FLATMAP").name("print-temperature-warnings");

        // ---- keyBy ----------------------------------------------------------------------------------------------
        // The lambda extracts the key from a record. Watch the number after the colon in the output:
        // sensor-1 is ALWAYS printed by the same worker, sensor-2 by the same worker, and so on.
        // (keyBy is not an operator - it only decides where records go - so there is nothing to name; the print sink
        // that follows it is the operator that runs after the shuffle.)
        valid.keyBy(reading -> reading.sensorId).print("KEYBY").name("print-keyed-by-sensorid");

        // Nothing has run yet: the lines above only DESCRIBE the pipeline. execute() starts it.
        env.getExecutionPlan();
        env.execute("Lesson 1 - basic operators");

    }

    /**
     * A flatMap gets a Collector and calls collect(...) as often as it likes.
     * Below 28 degrees: no output at all. From 28: one output. Above 32: two outputs.
     * (Writing flatMap as a class - instead of a lambda - keeps the input and output types easy to read.)
     * Example outputs
     * FLATMAP:2> sensor-2 is warm (33.1C)
     * FLATMAP:2> sensor-2 is DANGEROUSLY HOT (33.1C)
     */
    static class WarningFlatMap implements FlatMapFunction<SensorReading, String> {
        @Override
        public void flatMap(SensorReading reading, Collector<String> out) {
            if (reading.temperature >= 28) {
                out.collect(reading.sensorId + " is warm (" + reading.temperature + "C)");
            }
            if (reading.temperature > 32) {
                out.collect(reading.sensorId + " is DANGEROUSLY HOT (" + reading.temperature + "C)");
            }
        }
    }
}

