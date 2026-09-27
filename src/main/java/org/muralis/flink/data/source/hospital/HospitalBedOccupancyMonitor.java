package org.muralis.flink.data.source.hospital;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.muralis.flink.data.model.AdmitEvent;
import org.muralis.flink.data.model.BedOccupancyAlert;
import org.muralis.flink.data.model.BedOccupancyStatus;
import org.muralis.flink.data.model.DischargeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks each hospital's current bed occupancy from two connected streams, keyed by
 * {@code hospitalID}: admissions ({@code +1}) and discharges ({@code -1}). Emits a
 * {@link BedOccupancyAlert} whenever the hospital's {@link BedOccupancyStatus} changes.
 *
 * <p>Status transitions use hysteresis, so occupancy that hovers around a boundary does not cause
 * an alert storm:
 * <pre>
 *   Status   Enter when                  Leave (downward) when
 *   NORMAL   ratio &lt; 0.85              -
 *   HIGH     ratio &gt;= 0.90             ratio &lt; 0.85
 *   FULL     ratio &gt;= 1.00             ratio &lt; 0.95
 * </pre>
 *
 * <p>A discharge for a hospital with zero occupied beds is floored at 0 but counted as
 * {@code occupancyUnderflows}: it means something is wrong upstream, because the discharge's
 * admission is always forwarded before the discharge is emitted.
 *
 * <p>Unlike {@link HospitalCapacityMonitor} (Lesson 2A), occupancy here goes down as well as up.
 */
public class HospitalBedOccupancyMonitor extends KeyedCoProcessFunction<String, AdmitEvent, DischargeEvent, BedOccupancyAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(HospitalBedOccupancyMonitor.class);

    /** Enter {@link BedOccupancyStatus#HIGH} at or above this ratio. */
    public static final double HIGH_ENTER_RATIO = HospitalCapacityRegistry.ALERT_THRESHOLD_RATIO;
    /** Leave {@link BedOccupancyStatus#HIGH} (down to NORMAL) below this ratio. */
    public static final double HIGH_EXIT_RATIO = 0.85;
    /** Enter {@link BedOccupancyStatus#FULL} at or above this ratio. */
    public static final double FULL_ENTER_RATIO = 1.0;
    /** Leave {@link BedOccupancyStatus#FULL} (down to HIGH) below this ratio. */
    public static final double FULL_EXIT_RATIO = 0.95;

    private transient ValueState<Integer> occupied;
    private transient ValueState<BedOccupancyStatus> status;
    private transient Counter occupancyUnderflows;

    @Override
    public void open(OpenContext openContext) throws Exception {
        occupied = getRuntimeContext().getState(new ValueStateDescriptor<>("occupiedBeds", Integer.class));
        status = getRuntimeContext().getState(new ValueStateDescriptor<>("bedOccupancyStatus", BedOccupancyStatus.class));
        occupancyUnderflows = getRuntimeContext().getMetricGroup().counter("occupancyUnderflows");
    }

    @Override
    public void processElement1(AdmitEvent admitEvent, Context ctx, Collector<BedOccupancyAlert> out) throws Exception {
        long eventTime = ctx.timestamp() != null ? ctx.timestamp() : admitEvent.getAdmitTime();
        updateOccupancy(admitEvent.getHospitalID(), +1, eventTime, out);
    }

    @Override
    public void processElement2(DischargeEvent dischargeEvent, Context ctx, Collector<BedOccupancyAlert> out) throws Exception {
        long eventTime = ctx.timestamp() != null ? ctx.timestamp() : dischargeEvent.getDischargeTime();
        updateOccupancy(dischargeEvent.getHospitalID(), -1, eventTime, out);
    }

    private void updateOccupancy(String hospitalID, int delta, long eventTime, Collector<BedOccupancyAlert> out) throws Exception {
        int maxCapacity = HospitalCapacityRegistry.capacityForHospital(hospitalID);
        if (maxCapacity <= 0) {
            return; // unknown hospital: no capacity to measure against
        }

        int current = occupied.value() != null ? occupied.value() : 0;
        int updated = current + delta;
        if (updated < 0) {
            occupancyUnderflows.inc();
            LOG.warn("Discharge from hospital {} with no occupied beds; flooring occupancy at 0", hospitalID);
            updated = 0;
        }
        occupied.update(updated);

        BedOccupancyStatus previous = status.value() != null ? status.value() : BedOccupancyStatus.NORMAL;
        double ratio = (double) updated / maxCapacity;
        BedOccupancyStatus next = nextStatus(previous, ratio);
        if (next != previous) {
            status.update(next);
            out.collect(new BedOccupancyAlert(
                    hospitalID,
                    HospitalCapacityRegistry.regionForHospital(hospitalID),
                    previous,
                    next,
                    updated,
                    maxCapacity,
                    ratio * 100.0,
                    eventTime));
        }
    }

    /** Applies the hysteresis table above to move from {@code current} given the new occupancy ratio. */
    static BedOccupancyStatus nextStatus(BedOccupancyStatus current, double ratio) {
        if (ratio >= FULL_ENTER_RATIO) {
            return BedOccupancyStatus.FULL;
        }
        switch (current) {
            case FULL:
                if (ratio >= FULL_EXIT_RATIO) {
                    return BedOccupancyStatus.FULL;
                }
                return ratio >= HIGH_EXIT_RATIO ? BedOccupancyStatus.HIGH : BedOccupancyStatus.NORMAL;
            case HIGH:
                return ratio >= HIGH_EXIT_RATIO ? BedOccupancyStatus.HIGH : BedOccupancyStatus.NORMAL;
            case NORMAL:
            default:
                return ratio >= HIGH_ENTER_RATIO ? BedOccupancyStatus.HIGH : BedOccupancyStatus.NORMAL;
        }
    }
}
