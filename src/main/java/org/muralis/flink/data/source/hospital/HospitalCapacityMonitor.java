package org.muralis.flink.data.source.hospital;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.muralis.flink.data.model.AdmitEvent;
import org.muralis.flink.data.model.CapacityAlert;

/**
 * For each hospital, keep a running count of occupied beds. When that hospital reaches its capacity-alert threshold, emit one alert, 
 * while continuing to pass the original admission event downstream.
 * 
 * Keyed process function (keyed by {@code hospitalID}) that maintains a running admission count
 * per hospital and raises a one-time {@link CapacityAlert} on a side output once cumulative
 * occupancy first reaches {@link HospitalCapacityRegistry#ALERT_THRESHOLD_RATIO} of the
 * hospital's maximum bed capacity.
 *
 * <p>Because the simulation does not model discharges, occupancy here is the cumulative count of
 * admissions observed since the job started for that hospital; it therefore only grows, so the
 * alert is guarded to fire only once per hospital.
 *
 * <p>Every incoming {@link AdmitEvent} is always forwarded unchanged on the main output, so
 * this function can be inserted into the pipeline without altering existing consumers.
 */
public class HospitalCapacityMonitor extends KeyedProcessFunction<String, AdmitEvent, AdmitEvent> {

    /** Side output tag carrying capacity alerts. Read via {@code SingleOutputStreamOperator.getSideOutput(...)}. */
    public static final OutputTag<CapacityAlert> CAPACITY_ALERT_TAG = new OutputTag<CapacityAlert>("capacity-alerts") {};

    private transient ValueState<Long> occupiedBeds;
    private transient ValueState<Boolean> alerted;

    @Override
    public void open(OpenContext openContext) throws Exception {
        occupiedBeds = getRuntimeContext().getState(new ValueStateDescriptor<>("occupiedBeds", Long.class));
        alerted = getRuntimeContext().getState(new ValueStateDescriptor<>("capacityAlerted", Boolean.class));
    }

    @Override
    public void processElement(AdmitEvent admitEvent, Context ctx, Collector<AdmitEvent> out) throws Exception {
        long currentOccupancy = (occupiedBeds.value() != null) ? occupiedBeds.value() : 0L;
        currentOccupancy += 1;
        occupiedBeds.update(currentOccupancy);

        // Check if a capacity alert has already been raised for this hospital.
        // This expression is null-safe. If alerted.value() returns null, .equals() simply returns false without throwing an exception.
        boolean alreadyAlerted = Boolean.TRUE.equals(alerted.value());
        int maxCapacity = HospitalCapacityRegistry.capacityForHospital(admitEvent.getHospitalID());

        if (!alreadyAlerted && maxCapacity > 0) {
            double occupancyRatio = (double) currentOccupancy / maxCapacity;
            if (occupancyRatio >= HospitalCapacityRegistry.ALERT_THRESHOLD_RATIO) {
                ctx.output(CAPACITY_ALERT_TAG, new CapacityAlert(
                        admitEvent.getHospitalID(),
                        admitEvent.getRegionID(),
                        currentOccupancy,
                        maxCapacity,
                        occupancyRatio * 100.0,
                        admitEvent.getAdmitTime()
                ));
                alerted.update(true);
            }
        }

        out.collect(admitEvent);
    }
}