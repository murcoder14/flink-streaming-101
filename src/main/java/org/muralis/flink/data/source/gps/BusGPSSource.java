package org.muralis.flink.data.source.gps;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.muralis.flink.data.model.BusGPSEvent;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

public class BusGPSSource implements Source<BusGPSEvent, BusGPSSource.NoSplit, Boolean> {

    public static class NoSplit implements SourceSplit, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public String splitId() {
            return "city-bus-gps-feed-0";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NoSplit noSplit = (NoSplit) o;
            return Objects.equals(splitId(), noSplit.splitId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(splitId());
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<BusGPSEvent, NoSplit> createReader(SourceReaderContext readerContext) {
        return new BusGPSSourceReader(readerContext);
    }

    @Override
    public SplitEnumerator<NoSplit, Boolean> createEnumerator(SplitEnumeratorContext<NoSplit> enumContext) {
        return new BusGPSSplitEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<NoSplit, Boolean> restoreEnumerator(SplitEnumeratorContext<NoSplit> enumContext, Boolean checkpoint) {
        return new BusGPSSplitEnumerator(enumContext, checkpoint != null && checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NoSplit> getSplitSerializer() {
        return new NoSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Boolean> getEnumeratorCheckpointSerializer() {
        return new BooleanSerializer();
    }

    // ------------------------------------------------------------
    // SERIALIZERS
    // ------------------------------------------------------------
    private static class NoSplitSerializer implements SimpleVersionedSerializer<NoSplit> {
        @Override public int getVersion() { return 1; }
        @Override public byte[] serialize(NoSplit obj) { return new byte[0]; }
        @Override public NoSplit deserialize(int version, byte[] serialized) { return new NoSplit(); }
    }

    private static class BooleanSerializer implements SimpleVersionedSerializer<Boolean> {
        @Override public int getVersion() { return 1; }
        @Override public byte[] serialize(Boolean obj) { return new byte[] { (byte) (Boolean.TRUE.equals(obj) ? 1 : 0) }; }
        @Override public Boolean deserialize(int version, byte[] serialized) { return serialized != null && serialized.length > 0 && serialized[0] == 1; }
    }
}