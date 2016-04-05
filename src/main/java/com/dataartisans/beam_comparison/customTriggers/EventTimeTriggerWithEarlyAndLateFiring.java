package com.dataartisans.beam_comparison.customTriggers;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.IOException;

public class EventTimeTriggerWithEarlyAndLateFiring extends Trigger<Object, TimeWindow> {

	private static final long serialVersionUID = 1L;

	private final boolean accumulating;
	private final long allowedLateness;
	private final long earlyFiringPeriod;
	private final long lateFiringPeriod;

	private final ValueStateDescriptor<Long> processingTimeTriggerDescriptor =
			new ValueStateDescriptor<Long>("PROCESSING_TIME_TRIGGER", LongSerializer.INSTANCE, -1L);

	private final ValueStateDescriptor<Boolean> isPastTheEndOfWindow =
			new ValueStateDescriptor<Boolean>("AFTER_END_OF_WINDOW", BooleanSerializer.INSTANCE, false);

	public static EventTimeTriggerWithEarlyAndLateFiring create() {
		return new EventTimeTriggerWithEarlyAndLateFiring(false, 0L, 0L, 0L);
	}

	private EventTimeTriggerWithEarlyAndLateFiring(boolean accumulating, long allowedLateness, long earlyFiring, long lateFiring) {
		Preconditions.checkArgument(earlyFiring >= 0);
		Preconditions.checkArgument(lateFiring >= 0);
		Preconditions.checkArgument(allowedLateness >= 0);

		this.allowedLateness = allowedLateness;
		this.earlyFiringPeriod = earlyFiring;
		this.lateFiringPeriod = lateFiring;
		this.accumulating = accumulating;
	}

	public EventTimeTriggerWithEarlyAndLateFiring withAllowedLateness(Time allowedLateness) {
		return new EventTimeTriggerWithEarlyAndLateFiring(accumulating, allowedLateness.toMilliseconds(), earlyFiringPeriod, lateFiringPeriod);
	}

	public EventTimeTriggerWithEarlyAndLateFiring withEarlyFiringEvery(Time earlyFiringPeriod) {
		return new EventTimeTriggerWithEarlyAndLateFiring(accumulating, allowedLateness, earlyFiringPeriod.toMilliseconds(), lateFiringPeriod);
	}

	public EventTimeTriggerWithEarlyAndLateFiring withLateFiringEvery(Time lateFiringPeriod) {
		return new EventTimeTriggerWithEarlyAndLateFiring(accumulating, allowedLateness, earlyFiringPeriod, lateFiringPeriod.toMilliseconds());
	}

	public EventTimeTriggerWithEarlyAndLateFiring accumulating() {
		return new EventTimeTriggerWithEarlyAndLateFiring(true, allowedLateness, earlyFiringPeriod, lateFiringPeriod);
	}

	public EventTimeTriggerWithEarlyAndLateFiring discarding() {
		return new EventTimeTriggerWithEarlyAndLateFiring(false, allowedLateness, earlyFiringPeriod, lateFiringPeriod);
	}

	@Override
	public TriggerResult onElement(Object o, long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
		if (timeWindow.maxTimestamp() + allowedLateness <= triggerContext.getCurrentWatermark()) {
			return TriggerResult.PURGE;
		}

		triggerContext.registerEventTimeTimer(timeWindow.maxTimestamp());

		ValueState<Long> processingTimeTrigger = triggerContext.getPartitionedState(processingTimeTriggerDescriptor);
		if(processingTimeTrigger.value() == -1L) {
			long nextProcessingTimeTriggerTimestamp = getNextProcessingTimeTriggerTimestamp(triggerContext);
			processingTimeTrigger.update(nextProcessingTimeTriggerTimestamp);
			triggerContext.registerProcessingTimeTimer(nextProcessingTimeTriggerTimestamp);
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
		ValueState<Long> processingTimeTrigger = triggerContext.getPartitionedState(processingTimeTriggerDescriptor);
		long nextProcessingTimeTriggerTimestamp = getNextProcessingTimeTriggerTimestamp(triggerContext);

		processingTimeTrigger.update(nextProcessingTimeTriggerTimestamp);
		triggerContext.registerProcessingTimeTimer(nextProcessingTimeTriggerTimestamp);
		return TriggerResult.FIRE;
	}

	private long getNextProcessingTimeTriggerTimestamp(TriggerContext triggerContext) throws IOException {
		ValueState<Boolean> isAfterEndOfWindow = triggerContext.getPartitionedState(isPastTheEndOfWindow);
		boolean afterEndOfWindow = isAfterEndOfWindow.value();
		return afterEndOfWindow ?
				System.currentTimeMillis() + lateFiringPeriod :
				System.currentTimeMillis() + earlyFiringPeriod;
	}

	@Override
	public TriggerResult onEventTime(long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
		if (time == timeWindow.maxTimestamp()) {

			// mark that we have passed the end of the window in event time
			ValueState<Boolean> endOfWindowToProcessing = triggerContext.getPartitionedState(isPastTheEndOfWindow);
			boolean afterEndOfWindow = endOfWindowToProcessing.value();
			if(afterEndOfWindow) {
				throw new RuntimeException("The end of window for this window has already been found.");
			}
			endOfWindowToProcessing.update(true);

			// remove the already registered timer and update the processing time trigger
			// so that it fires with the late firing interval
			ValueState<Long> processingTimeTrigger = triggerContext.getPartitionedState(processingTimeTriggerDescriptor);
			long timerToRemove = processingTimeTrigger.value();
			if(timerToRemove != -1) {
				triggerContext.deleteProcessingTimeTimer(timerToRemove);
			}
			long nextProcessingTimeTriggerTimestamp = getNextProcessingTimeTriggerTimestamp(triggerContext);
			processingTimeTrigger.update(nextProcessingTimeTriggerTimestamp);
			triggerContext.registerProcessingTimeTimer(nextProcessingTimeTriggerTimestamp);

			if (accumulating) {
				// register the cleanup timer if we are accumulating (and allow lateness)
				if (allowedLateness > 0) {
					triggerContext.registerEventTimeTimer(timeWindow.maxTimestamp() + allowedLateness);
				}
				return TriggerResult.FIRE;
			} else {
				return TriggerResult.FIRE_AND_PURGE;
			}
		} else if (time == timeWindow.maxTimestamp() + allowedLateness) {
			return TriggerResult.PURGE;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public void clear(TimeWindow window, TriggerContext triggerContext) throws Exception {

		// delete the registered processing time timer
		ValueState<Long> processingTimeTrigger = triggerContext.getPartitionedState(processingTimeTriggerDescriptor);
		long late = processingTimeTrigger.value();
		if(late != -1) {
			triggerContext.deleteProcessingTimeTimer(late);
		}
		processingTimeTrigger.clear();

		// delete the flag showing that we are past the end_of_window in event time
		ValueState<Boolean> isPastEndOfWindow = triggerContext.getPartitionedState(isPastTheEndOfWindow);
		isPastEndOfWindow.clear();

		// finally delete the regisetered event time timers.
		triggerContext.deleteEventTimeTimer(window.maxTimestamp());
		triggerContext.deleteEventTimeTimer(window.maxTimestamp() + allowedLateness);
	}
}