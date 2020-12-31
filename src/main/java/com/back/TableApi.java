package com.back;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

public class TableApi {

	public static void main(String[] args) throws Exception {
		// blink streaming query
		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment, bsSettings);

		String sqlUpdateStr = "CREATE TABLE MyUserTable (\r\n" + "  -- declare the schema of the table\r\n"
				+ "  id BIGINT,\r\n" + "  name STRING,\r\n" + "  age BIGINT\r\n" + ") WITH (\r\n"
				+ "  -- declare the external system to connect to\r\n" + "  'connector.type' = 'kafka',\r\n"
				+ "  'connector.version' = '0.10',\r\n" + "  'connector.topic' = 'wyh-avro-topic',\r\n"
				+ "  'connector.startup-mode' = 'earliest-offset',\r\n"
				+ "  'connector.properties.zookeeper.connect' = '172.16.6.163:2181',\r\n"
				+ "  'connector.properties.bootstrap.servers' = '172.16.6.163:9092',\r\n" + "\r\n"
				+ "  -- specify the update-mode for streaming tables\r\n" + "  'update-mode' = 'append',\r\n" + "\r\n"
				+ "  -- declare a format for this system\r\n" + "  'format.type' = 'avro',\r\n"
				+ "  'format.avro-schema' = '{\r\n" + "							\"type\": \"record\",\r\n"
				+ "							\"name\": \"Student\",\r\n" + "							\"fields\": [\r\n"
				+ "								{\r\n" + "									\"name\": \"id\",\r\n"
				+ "									\"type\": \"int\"\r\n" + "								},\r\n"
				+ "								{\r\n" + "									\"name\": \"name\",\r\n"
				+ "									\"type\": \"string\"\r\n" + "								},\r\n"
				+ "								{\r\n" + "									\"name\": \"age\",\r\n"
				+ "									\"type\": \"int\"\r\n" + "								}\r\n"
				+ "							]\r\n" + "						}'\r\n" + ")";

		tableEnv.sqlUpdate(sqlUpdateStr);

		tableEnv.connect(
				new Kafka().version("0.10").topic("wyh-avro-topic").property("zookeeper.connect", "172.16.6.163:2181")
						.property("group.id", "click-group").startFromEarliest())
				.withFormat(new Json().jsonSchema("{...}").failOnMissingField(false))
				.withSchema(new Schema().field("id", DataTypes.VARCHAR(30)).from("u_name")
						.field("name", DataTypes.DECIMAL(30, 2)).field("age", DataTypes.TIMESTAMP(3)).proctime()
//		       .field("age", DataTypes.TIMESTAMP(3)).rowtime(new Rowtime().timestampsFromField("time"))
						// 可以通过字段注释来注册 flink watermark
						.field("age", DataTypes.TIMESTAMP(3))
						.rowtime(new Rowtime().watermarksPeriodicBounded(11).timestampsFromField("time"))
				).inAppendMode().createTemporaryTable("MyTable");
		
		
		
		

	}
}
