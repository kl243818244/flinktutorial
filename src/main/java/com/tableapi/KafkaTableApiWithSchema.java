package com.tableapi;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class KafkaTableApiWithSchema {
	
	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment,fsSettings);
		
		tableEnv.connect(new Kafka()
				.version("0.10")
				.topic("wyh-avro-topic")
				.startFromEarliest()
				.property("bootstrap.servers","172.16.6.163:9092")
				.property("zookeeper.connect", "172.16.6.163:2181"))
		.withFormat(new Avro()
				.avroSchema("{\r\n" + 
						"    \"type\": \"record\",\r\n" + 
						"    \"name\": \"Student\",\r\n" + 
						"    \"fields\": [\r\n" + 
						"        {\r\n" + 
						"            \"name\": \"id\",\r\n" + 
						"            \"type\": \"int\"\r\n" + 
						"        },\r\n" + 
						"        {\r\n" + 
						"            \"name\": \"name\",\r\n" + 
						"            \"type\": \"string\"\r\n" + 
						"        },\r\n" + 
						"        {\r\n" + 
						"            \"name\": \"age\",\r\n" + 
						"            \"type\": \"int\"\r\n" + 
						"        },\r\n" + 
						"        {\r\n" + 
						"            \"name\": \"timestamp\",\r\n" + 
						"            \"type\": {\r\n" + 
						"                \"type\": \"long\",\r\n" + 
						"                \"logicalType\": \"timestamp-millis\"\r\n" + 
						"            }\r\n" + 
						"        }\r\n" + 
						"    ]\r\n" + 
						"}"))
		.withSchema(new Schema()
				.field("timestamp", DataTypes.TIMESTAMP(3))
		        .rowtime(new Rowtime()
		          .timestampsFromField("timestamp")
		          .watermarksPeriodicBounded(60000)
		        )
				.field("id", DataTypes.INT())
				.field("name", DataTypes.STRING())
				.field("age", DataTypes.INT()))
		.createTemporaryTable("myUserTable");;
		
		String sql = "select id,name,age from myUserTable";
		
		Table sqlQuery = tableEnv.sqlQuery(sql);
		
		// table api 方式
//		Table select = tableEnv.from("inputtable").select("");
		
		DataStream<Row> appendStream = tableEnv.toAppendStream(sqlQuery, Row.class);
		
		appendStream.print("==============>");
		
		executionEnvironment.execute();
	}

}
