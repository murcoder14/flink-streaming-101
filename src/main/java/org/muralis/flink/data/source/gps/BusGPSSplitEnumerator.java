package org.muralis.flink.data.source.gps;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * BusGPSSplitEnumerator decides which Flink worker gets access to the bus GPS feed.
 * Responsible for discovering the source splits, and assigning them to the SourceReader.
 */
public class BusGPSSplitEnumerator implements SplitEnumerator<BusGPSSource.NoSplit, Boolean> {

    // A context class for the SplitEnumerator. This class serves the following purposes:
    //      Host information necessary for the SplitEnumerator to make split assignment decisions.
    //      Accept and track the split assignment from the enumerator.
    //      Provide a managed threading model so the split enumerators do not need to create their own internal threads.
    private final SplitEnumeratorContext<BusGPSSource.NoSplit> context;

    // "Have I already given the bus GPS feed to somebody?"
    private boolean splitAssigned;

    public BusGPSSplitEnumerator(SplitEnumeratorContext<BusGPSSource.NoSplit> context) {
        this(context, false);
    }

    public BusGPSSplitEnumerator(SplitEnumeratorContext<BusGPSSource.NoSplit> context, boolean splitAssigned) {
        this.context = context;
        this.splitAssigned = splitAssigned;
    }

    @Override
    public void start() {}

    @Override
    public synchronized void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        assignSplitOrSignalNoMore(subtaskId);
    }

    @Override
    public synchronized void addReader(int subtaskId) {
        assignSplitOrSignalNoMore(subtaskId);
    }

    private void assignSplitOrSignalNoMore(int subtaskId) {
        if (!splitAssigned) {
            context.assignSplit(new BusGPSSource.NoSplit(), subtaskId);
            splitAssigned = true;
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public synchronized void addSplitsBack(List<BusGPSSource.NoSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            splitAssigned = false;
            Map<Integer, ReaderInfo> readers = context.registeredReaders();
            if (!readers.isEmpty()) {
                int nextSubtask = readers.keySet().iterator().next();
                context.assignSplit(splits.get(0), nextSubtask);
                splitAssigned = true;
            }
        }
    }

    @Override
    public synchronized Boolean snapshotState(long checkpointId) {
        return splitAssigned;
    }

    @Override
    public void close() throws IOException {}
}
