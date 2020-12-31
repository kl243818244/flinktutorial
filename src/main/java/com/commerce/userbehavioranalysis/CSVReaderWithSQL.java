package com.commerce.userbehavioranalysis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class CSVReaderWithSQL {

	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment, fsSettings);

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
		
		tableEnv.createTemporaryView("user_behavior", assignTimestampsAndWatermarks , "userId as user_id, itemId as item_id, categoryId as category_id, behavior, ts.rowtime");
		
		// 每隔 5分钟 统计最近一小时每个商品的点击量
		// group by itemId , HOP(ts, INTERVAL '2' second, INTERVAL '10' second)
		String hopWindowSql = "select item_id,count(item_id) , HOP_START(ts, INTERVAL '15' second, INTERVAL '30' second) from user_behavior group by item_id , HOP(ts, INTERVAL '15' second, INTERVAL '30' second)";
		
		Table hopWindowSqlTable = tableEnv.sqlQuery(hopWindowSql);
		
		DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(hopWindowSqlTable, Row.class);
		
		retractStream.print("=============> UserBehaviorRetractStream");
		
		executionEnvironment.execute();
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
