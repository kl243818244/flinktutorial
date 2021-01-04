package com.commerce.userbehavioranalysis;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
		
//		executionEnvironment.setParallelism(1);

		// 1.12.0 已经是默认 EventTime
//		executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		WatermarkStrategy<UserBehavior> withTimestampAssigner = WatermarkStrategy
		        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(20))
		        .withTimestampAssigner((event, timestamp) -> event.getTs());
		
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
		}).returns(UserBehavior.class).assignTimestampsAndWatermarks(withTimestampAssigner);
		
		tableEnv.createTemporaryView("user_behavior", assignTimestampsAndWatermarks , "userId as user_id, itemId as item_id, categoryId as category_id, behavior, ts.rowtime");
		
		String sql = "select category_id , cnt from ( select rownum , category_id , cnt from (\r\n" + 
				"	select category_id , cnt ,  ROW_NUMBER() OVER ( ORDER BY cnt DESC  ) AS rownum from ( select category_id , count(category_id) as cnt from user_behavior group by category_id )\r\n" + 
				")\r\n" + 
				"where rownum <= 2 )";
		
		Table sqlQuery = tableEnv.sqlQuery(sql);
		
		DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(sqlQuery, Row.class);
		
		retractStream.print("====>");
		
//		retractStream.writeAsText("F:\\weining\\2021-1月份工作计划\\test.txt");
		
		// 默认是 upsert 模式
		
		String sinkDDL = "create table sensor_count \r\n" + 
				"(\r\n" + 
				"	category_id int ,\r\n" + 
				"	cnt bigint not null , \r\n" + 
				"	primary key(category_id) NOT ENFORCED \r\n" + 
				")\r\n" + 
				"with\r\n" + 
				"(\r\n" + 
				" 'connector' = 'jdbc',\r\n" + 
				" 'url' = 'jdbc:mysql://172.16.6.163:3306/flink',\r\n" + 
				" 'table-name' = 'sensor_count',\r\n" + 
				" 'driver' = 'com.mysql.jdbc.Driver',\r\n" + 
				" 'username' = 'root',\r\n" + 
				" 'password' = '123456'\r\n" + 
				")\r\n" + 
				"";
		
		tableEnv.executeSql(sinkDDL);
		
		
		sqlQuery.executeInsert("sensor_count");
		
/*		String insertIntoSql = "insert into sensor_count (select rownum , category_id , cnt from (\r\n" + 
				"	select category_id , cnt ,  ROW_NUMBER() OVER ( ORDER BY cnt DESC  ) AS rownum from ( select category_id , count(category_id) as cnt from user_behavior group by category_id )\r\n" + 
				")\r\n" + 
				"where rownum <= 2) ";
		
		tableEnv.executeSql(insertIntoSql);*/
		
		executionEnvironment.execute();
	}
	
}
