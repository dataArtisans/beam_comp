package com.dataartisans.beam_comparison;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * A batch job which calculates per-user score totals over a bounded set of input data.
 * */
public class UserScores {

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
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<ScoreEvent> userScores = env
				.readTextFile(args[0])
				.flatMap(new InputParser())
				.groupBy("userId")
				.reduce(new ReduceFunction<ScoreEvent>() {
					@Override
					public ScoreEvent reduce(ScoreEvent value1, ScoreEvent value2) throws Exception {
						return new ScoreEvent(
								value1.getUserId(),
								value1.getTeamId(),
								value1.getScore() + value2.getScore(),
								value1.getTimestamp());
					}
				});

		userScores.print();
	}
}
