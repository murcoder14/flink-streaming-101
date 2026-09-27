package org.muralis.flink.launcher;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;

public class Lesson1A {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        /**
         * In Apache Flink, implementing the Source interface directly (the FLIP-27 source model) requires implementing three components: a Source, a SourceReader,
         * and a SplitEnumerator.
         *
         * For data generation, the idiomatic approach in Flink is to use the DataGeneratorSource, which already implements the new Source interface under the
         * hood and handles source splits, state checkpointing, and thread management automatically.
         *
         * It allows us to supply a GeneratorFunction.
         */
        GeneratorFunction<Long, String> generatorFunction = index -> "Word:" + index;
        DataGeneratorSource<String> source = new DataGeneratorSource<>(generatorFunction, Long.MAX_VALUE, RateLimiterStrategy.perSecond(50), Types.STRING);
        DataStreamSource<String> words = env.fromSource(source, WatermarkStrategy.noWatermarks(), "string-generator");

        words.map(word -> 1L)
                .returns(Types.LONG)
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(15)))
                .reduce(Long::sum)
                .print()
                .name("mapper-window-word-counter");
        env.execute("Word Counter Job");
    }
}