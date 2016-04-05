package com.dataartisans.beam_comparison.customTriggers;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class AccumulatingProcessingTimeTrigger extends Trigger<Object, GlobalWindow> {

	private static final long serialVersionUID = 1L;

	private final long period;

	private final ValueStateDescriptor<Long> processingTimeTriggerDescriptor =
			new ValueStateDescriptor<Long>("PROCESSING_TIME_TRIGGER", LongSerializer.INSTANCE, -1L);

	public static AccumulatingProcessingTimeTrigger create(Time period) {
		return new AccumulatingProcessingTimeTrigger(period.toMilliseconds());
	}

	private AccumulatingProcessingTimeTrigger(long period) {
		Preconditions.checkArgument(period >= 0);
		this.period = period;
	}

	@Override
	public TriggerResult onElement(Object o, long time, GlobalWindow window, TriggerContext triggerContext) throws Exception {
		// set the first processing time trigger
		ValueState<Long> processingTimeTrigger = triggerContext.getPartitionedState(processingTimeTriggerDescriptor);
		if(processingTimeTrigger.value() == -1L) {
			long nextProcessingTimeTriggerTimestamp = getNextProcessingTimeTriggerTimestamp();
			processingTimeTrigger.update(nextProcessingTimeTriggerTimestamp);
			triggerContext.registerProcessingTimeTimer(nextProcessingTimeTriggerTimestamp);
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext triggerContext) throws Exception {
		ValueState<Long> processingTimeTrigger = triggerContext.getPartitionedState(processingTimeTriggerDescriptor);
		long nextProcessingTimeTriggerTimestamp = getNextProcessingTimeTriggerTimestamp();
		processingTimeTrigger.update(nextProcessingTimeTriggerTimestamp);
		triggerContext.registerProcessingTimeTimer(nextProcessingTimeTriggerTimestamp);
		return TriggerResult.FIRE;
	}

	private long getNextProcessingTimeTriggerTimestamp() {
		return System.currentTimeMillis() + period;
	}

	@Override
	public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext triggerContext) throws Exception {
		return TriggerResult.CONTINUE;
	}

	@Override
	public void clear(GlobalWindow window, TriggerContext triggerContext) throws Exception {

		// delete the registered processing time timer
		ValueState<Long> processingTimeTrigger = triggerContext.getPartitionedState(processingTimeTriggerDescriptor);
		long late = processingTimeTrigger.value();
		if(late != -1) {
			triggerContext.deleteProcessingTimeTimer(late);
		}
		processingTimeTrigger.clear();
	}
}
