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

import com.commerce.userbehavioranalysis.CSVReaderWithSQL.UserBehavior;

/**
 * 使用 SQL 方式计算topN
 *
 */
public class TOPNHotItemsWithSQL {
	
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
		
		String sql = "select rownum , category_id , cnt from (\r\n" + 
				"	select category_id , cnt ,  ROW_NUMBER() OVER ( ORDER BY cnt DESC  ) AS rownum from ( select category_id , count(category_id) as cnt from user_behavior group by category_id )\r\n" + 
				")\r\n" + 
				"where rownum <= 2";
		
		Table sqlQuery = tableEnv.sqlQuery(sql);
		
		DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(sqlQuery, Row.class);
		
		retractStream.print("====>");
		
//		retractStream.writeAsText("F:\\weining\\2021-1月份工作计划\\test.txt");
		
		String sinkDDL = "";
		
		
		
		
		
		
		
		executionEnvironment.execute();
	}
	
}
