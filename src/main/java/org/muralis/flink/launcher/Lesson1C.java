package org.muralis.flink.launcher;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.muralis.flink.data.model.BusGPSEvent;
import org.muralis.flink.data.source.gps.BusGPSSource;

public class Lesson1C {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        BusGPSSource busGPSSource = new BusGPSSource();
        DataStream<BusGPSEvent> busStream = env.fromSource(busGPSSource, WatermarkStrategy.noWatermarks(), "City Bus GPS Source");
        busStream.print("GPS").name("print-gps-events");
        env.execute("City Bus GPS Streaming");
    }
}
