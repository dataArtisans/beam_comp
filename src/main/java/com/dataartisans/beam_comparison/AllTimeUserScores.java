package com.dataartisans.beam_comparison;

import com.dataartisans.beam_comparison.customTriggers.AccumulatingProcessingTimeTrigger;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class AllTimeUserScores {

	private static class InputParser implements FlatMapFunction<String, ScoreEvent> {

		@Override
		public void flatMap(String s, Collector<ScoreEvent> collector) throws Exception {
			// we assume that the input is userId, teamId, score, timestamp
			String[] tokens = s.split("\\s");
			if(tokens.length != 4) {
				throw new RuntimeException("Unknown input format.");
			}
			collector.collect(new ScoreEvent(
					Long.parseLong(tokens[0]),
					Integer.parseInt(tokens[1]),
					Integer.parseInt(tokens[2]),
					Long.parseLong(tokens[3])));
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<ScoreEvent> teamHourlyScores = env
				.readTextFile(args[0])
				.flatMap(new InputParser())
				.keyBy("userId")
				.window(GlobalWindows.create())
				.trigger(AccumulatingProcessingTimeTrigger.create(Time.minutes(10)))
				.sum("score");
		teamHourlyScores.print();

		env.execute("User Global Scores.");
	}
}
