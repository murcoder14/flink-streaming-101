package org.muralis.flink.data.source.hospital;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.muralis.flink.data.model.AdmitEvent;
import org.muralis.flink.data.model.DischargeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulates each admitted patient's discharge. For every admission it schedules an
 * <b>event-time timer</b> at {@code admitTimestamp + lengthOfStay}; when the timer fires, the same,
 * real {@code patientID} is emitted as a {@link DischargeEvent} on {@link #DISCHARGE_TAG}.
 *
 * <p>Must be keyed by {@code patientID}. One key then holds at most one pending discharge and
 * exactly one timer. Keying by hospital instead would be subtly wrong: Flink de-duplicates timers
 * per (key, timestamp), so two patients of the same hospital discharged in the same millisecond
 * would share one timer and one of the discharges would be lost.
 *
 * <p>Event time (rather than processing time) is used because {@code admitTime} is already the
 * event timestamp, because records emitted from {@code onTimer} then carry the timer time as their
 * timestamp (processing-time timers erase it), and because tests can advance time with
 * watermarks. Event-time timers only fire when the watermark advances, so if admissions stop,
 * pending discharges stop too.
 *
 * <p>Every incoming {@link AdmitEvent} is forwarded unchanged on the main output.
 */
public class PatientLifecycleSimulator extends KeyedProcessFunction<String, AdmitEvent, AdmitEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(PatientLifecycleSimulator.class);

    /** Side output carrying discharges. Read via {@code SingleOutputStreamOperator.getSideOutput(...)}. */
    public static final OutputTag<DischargeEvent> DISCHARGE_TAG = new OutputTag<DischargeEvent>("discharges") {};

    /** State descriptor name. Do not rename without a state-migration plan: savepoints refer to it. */
    static final String PENDING_DISCHARGE_STATE = "pendingDischarge";

    private final LengthOfStaySampler lengthOfStaySampler;

    private transient ValueState<DischargeEvent> pendingDischarge;
    private transient Counter dischargesEmitted;
    private transient Counter duplicateAdmissions;

    public PatientLifecycleSimulator(LengthOfStaySampler lengthOfStaySampler) {
        this.lengthOfStaySampler = lengthOfStaySampler;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        pendingDischarge = getRuntimeContext().getState(new ValueStateDescriptor<>(PENDING_DISCHARGE_STATE, DischargeEvent.class));
        dischargesEmitted = getRuntimeContext().getMetricGroup().counter("dischargesEmitted");
        duplicateAdmissions = getRuntimeContext().getMetricGroup().counter("duplicateAdmissions");
    }

    @Override
    public void processElement(AdmitEvent admitEvent, Context ctx, Collector<AdmitEvent> out) throws Exception {
        DischargeEvent existing = pendingDischarge.value();
        if (existing != null) {
            // Re-admission of a patient who is still in hospital (patient IDs wrap after 900k
            // admissions). Keep the existing stay rather than silently overwriting it.
            duplicateAdmissions.inc();
            LOG.warn("Patient {} admitted to {} while still admitted to {} until {}; keeping the existing stay",
                    ctx.getCurrentKey(), admitEvent.getHospitalID(), existing.getHospitalID(), existing.getDischargeTime());
        } else {
            Long admitTimestamp = ctx.timestamp();
            if (admitTimestamp == null) {
                throw new IllegalStateException(
                        "PatientLifecycleSimulator needs event-time timestamps; assign a WatermarkStrategy on the source");
            }
            String patientID = ctx.getCurrentKey();
            long dischargeTimestamp = admitTimestamp + lengthOfStaySampler.lengthOfStayMillis(patientID, admitEvent.getRegionID());

            pendingDischarge.update(new DischargeEvent(patientID, admitEvent.getHospitalID(), admitEvent.getAdmitTime(), dischargeTimestamp));
            ctx.timerService().registerEventTimeTimer(dischargeTimestamp);
        }

        out.collect(admitEvent);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdmitEvent> out) throws Exception {
        DischargeEvent discharge = pendingDischarge.value();
        if (discharge == null) {
            return;
        }
        ctx.output(DISCHARGE_TAG, discharge);
        dischargesEmitted.inc();
        // The timer is the only cleanup mechanism (no state TTL): TTL would not delete the timer.
        pendingDischarge.clear();
    }
}
