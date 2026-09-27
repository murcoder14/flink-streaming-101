package org.muralis.flink.data.source.gps;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.muralis.flink.data.model.BusGPSEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

// SOURCE READER
public class BusGPSSourceReader implements SourceReader<BusGPSEvent, BusGPSSource.NoSplit> {

    private static final String[] ROUTES = {"10-Downtown", "20-Airport", "30-University", "40-Hospital"};
    private static final String[] BUS_IDS = {"BUS-101", "BUS-102", "BUS-103", "BUS-104"};

    private static final long INTERVAL_MS = 500L;

    private final SourceReaderContext context;
    private final List<BusGPSSource.NoSplit> assignedSplits = new ArrayList<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "bus-gps-source-reader-scheduler");
        t.setDaemon(true);
        return t;
    });

    private CompletableFuture<Void> availability;
    private boolean noMoreSplits = false;
    private long lastEmitTime = 0L;

    public BusGPSSourceReader(SourceReaderContext context) {
        this.context = context;
        this.availability = new CompletableFuture<>();
    }

    @Override
    public void start() {
        if (assignedSplits.isEmpty()) {
            context.sendSplitRequest();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<BusGPSEvent> output) {
        if (assignedSplits.isEmpty()) {
            if (noMoreSplits) {
                return InputStatus.END_OF_INPUT;
            }
            if (availability.isDone()) {
                availability = new CompletableFuture<>();
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        long now = System.currentTimeMillis();
        long elapsed = now - lastEmitTime;

        if (lastEmitTime > 0 && elapsed < INTERVAL_MS) {
            scheduleNextAvailability(INTERVAL_MS - elapsed);
            return InputStatus.NOTHING_AVAILABLE;
        }

        lastEmitTime = now;
        emitRecord(output, now);

        scheduleNextAvailability(INTERVAL_MS);
        return InputStatus.NOTHING_AVAILABLE;
    }

    private void scheduleNextAvailability(long delayMs) {
        if (availability.isDone()) {
            CompletableFuture<Void> nextAvailability = new CompletableFuture<>();
            scheduler.schedule(() -> nextAvailability.complete(null), Math.max(0, delayMs), TimeUnit.MILLISECONDS);
            availability = nextAvailability;
        }
    }

    private void emitRecord(ReaderOutput<BusGPSEvent> output, long timestamp) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String busId = BUS_IDS[random.nextInt(BUS_IDS.length)];
        String route = ROUTES[random.nextInt(ROUTES.length)];
        double speedMph = Math.round(random.nextDouble(5, 45) * 10) / 10.0;
        double latitude = Math.round((41.75 + random.nextDouble(-0.03, 0.03)) * 10000.0) / 10000.0;
        double longitude = Math.round((-72.70 + random.nextDouble(-0.03, 0.03)) * 10000.0) / 10000.0;
        int passengers = random.nextInt(0, 50);

        output.collect(new BusGPSEvent(busId, route, latitude, longitude, speedMph, passengers, timestamp));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public void addSplits(List<BusGPSSource.NoSplit> splits) {
        assignedSplits.addAll(splits);
        availability.complete(null);
    }

    @Override
    public void notifyNoMoreSplits() {
        this.noMoreSplits = true;
        availability.complete(null);
    }

    @Override
    public List<BusGPSSource.NoSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(assignedSplits);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
