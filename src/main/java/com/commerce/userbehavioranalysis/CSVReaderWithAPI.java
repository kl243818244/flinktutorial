package com.commerce.userbehavioranalysis;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.util.Collector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class CSVReaderWithAPI {

	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStreamSource<String> readTextFile = executionEnvironment
				.readTextFile("D:\\STS\\STS4_WORKSPACES\\flinktutorial\\src\\main\\resources\\guigu\\UserBehavior.csv");

		SingleOutputStreamOperator<UserBehavior> assignTimestampsAndWatermarks = readTextFile.map(line -> {
			String[] split = line.split(",");

			UserBehavior userBehavior = new UserBehavior();
			userBehavior.setUserId(Long.valueOf(split[0]));
			userBehavior.setItemId(Long.valueOf(split[1]));
			userBehavior.setCategoryId(Integer.valueOf(split[2]));
			userBehavior.setBehavior(split[3]);
			userBehavior.setTs(Long.valueOf(split[4]));

			return userBehavior;
		}).returns(UserBehavior.class).assignTimestampsAndWatermarks(
				new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(1)) {
					private static final long serialVersionUID = 1L;

					@Override
					public long extractTimestamp(UserBehavior element) {
						return element.getTs() * 1000L;
					}
				});
		
		
		SingleOutputStreamOperator<ItemViewCount> aggregate = assignTimestampsAndWatermarks
		.keyBy("itemId")
		.timeWindow(Time.seconds(5), Time.seconds(10))
		.aggregate( new aggreFunc(),new WindowResultFunction());
		
		aggregate.print(" ===========> ");
		
		executionEnvironment.execute();
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ItemViewCount{
		private Long itemId;
		private Long windowEnd;
		private Long count;
	}
	
	
	public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 8770588149944953801L;

		@Override
		public void apply(Tuple key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out)
				throws Exception {
			Long itemId = key.getField(0);
			
			Long count = input.iterator().next();
			
			ItemViewCount itemViewCount = new ItemViewCount();
			
			itemViewCount.setItemId(itemId);
			
			itemViewCount.setWindowEnd(window.getEnd());
			
			itemViewCount.setCount(count);
			
			out.collect(itemViewCount);
		}
		
	}
	
	public static class aggreFunc implements AggregateFunction<UserBehavior,Long,Long>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 6841291843592140435L;

		
		@Override
		public Long createAccumulator() {
			// TODO Auto-generated method stub
			return 0L;
		}

		@Override
		public Long add(UserBehavior value, Long accumulator) {
			// TODO Auto-generated method stub
			return ++accumulator;
		}

		@Override
		public Long getResult(Long accumulator) {
			// TODO Auto-generated method stub
			return accumulator;
		}

		@Override
		public Long merge(Long a, Long b) {
			// TODO Auto-generated method stub
			return a + b;
		}
		
	}
	
	
	
	
	
	

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class UserBehavior {
		private Long userId;
		private Long itemId;
		private Integer categoryId;
		private String behavior;
		private Long ts;
	}

}
