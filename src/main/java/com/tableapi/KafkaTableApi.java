package com.tableapi;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class KafkaTableApi {

	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		
		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		
		executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment, fsSettings);

		tableEnv.connect(new Kafka().version("0.10").topic("sensor-json").startFromEarliest()
				.property("bootstrap.servers", "172.16.6.163:9092").property("zookeeper.connect", "172.16.6.163:2181"))
				.withFormat(new Json().failOnMissingField(false))
				.withSchema(new Schema().field("app_time", DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class))
						.rowtime(new Rowtime().timestampsFromField("app_time").watermarksPeriodicBounded(60000))
						.field("user_id", DataTypes.INT()))
				.createTemporaryTable("myUserTable");

		String sql = "select user_id,app_time from myUserTable";

		Table sqlQuery = tableEnv.sqlQuery(sql);

		// table api 方式
//		Table select = tableEnv.from("inputtable").select("");

		DataStream<Row> appendStream = tableEnv.toAppendStream(sqlQuery, Row.class);

		appendStream.print("==============>");

		executionEnvironment.execute();
	}

}
