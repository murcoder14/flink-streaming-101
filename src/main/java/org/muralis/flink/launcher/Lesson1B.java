package org.muralis.flink.launcher;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class Lesson1B {

    public static final int ELEMENTS_PER_SECOND = 100;
    public static final int WINDOW_SIZE = 5;

    // 1. GeneratorFunction that outputs random sentences or words
    public static class RandomWordGenerator implements GeneratorFunction<Long, String> {
        private static final String[] WORDS = {
                "The", "weary", "traveler", "who", "had", "traveled", "through",
                "the", "weary", "land", "for", "many", "weary", "days", "finally",
                "found", "the", "weary", "village", "where", "the", "weary",
                "people", "welcomed", "the", "weary", "traveler", "with", "the",
                "same", "weary", "smiles", "that", "the", "weary", "traveler",
                "had", "seen", "on", "the", "faces", "of", "the", "weary", "people",
                "in", "every", "weary", "village", "he", "had", "traveled",
                "through", "during", "his", "long", "and", "weary", "journey."
        };

        @Override
        public String map(Long value) {
            // Pick a random word from the lexicon
            int index = ThreadLocalRandom.current().nextInt(WORDS.length);
            return WORDS[index];
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Wrap GeneratorFunction in DataGeneratorSource (Implements FLIP-27 Source)
        DataGeneratorSource<String> source = new DataGeneratorSource<>(
                new RandomWordGenerator(),
                Long.MAX_VALUE,                        // Produce elements endlessly
                RateLimiterStrategy.perSecond(ELEMENTS_PER_SECOND),    // Rate limit: 100 elements/sec
                Types.STRING
        );

        // 3. Register Source and execute WordCount pipeline
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "random-word-source")
                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (word, out) -> {
                    out.collect(new Tuple2<>(word, 1L));
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(WINDOW_SIZE)))
                .reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1))
                .print();

        env.execute("Random Word Counting Job");
    }
}
