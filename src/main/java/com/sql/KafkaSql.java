package com.sql;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 *
 *使用 timestamp类型的方式
 *
 */
public class KafkaSql {
	
	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		
		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		
		executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment, fsSettings);
		
		String ddl = "CREATE TABLE MyUserTable (\r\n" + 
				"  user_id BIGINT,\r\n" + 
				"  item_id BIGINT,\r\n" + 
				"  behavior STRING,\r\n" + 
				"  temperature BIGINT,\r\n" + 
				"  app_time BIGINT,\r\n" + 
				"  ts AS TO_TIMESTAMP(FROM_UNIXTIME(app_time / 1000, 'yyyy-MM-dd HH:mm:ss')),\r\n" + 
				"  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\r\n" + 
				") WITH (\r\n" + 
				"  'connector.type' = 'kafka',\r\n" + 
				"  'connector.version' = '0.10',\r\n" + 
				"  'connector.topic' = 'sensor-json',\r\n" + 
				"  'connector.startup-mode' = 'earliest-offset',\r\n" + 
				"  'connector.properties.zookeeper.connect' = '172.16.6.163:2181',\r\n" + 
				"  'connector.properties.bootstrap.servers' = '172.16.6.163:9092',\r\n" + 
				"  'update-mode' = 'append',\r\n" + 
				"  'format.type' = 'json'\r\n" + 
				")";
		
		tableEnv.sqlUpdate(ddl);
		
		String sql = "select user_id,item_id,behavior,temperature,ts from MyUserTable";
		
		Table sqlQuery = tableEnv.sqlQuery(sql);
		
		DataStream<Row> appendStream = tableEnv.toAppendStream(sqlQuery, Row.class);
		
		appendStream.print();
		
		executionEnvironment.execute("========>");
	}

}
