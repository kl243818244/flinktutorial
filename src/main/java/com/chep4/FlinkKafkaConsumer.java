package com.chep4;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class FlinkKafkaConsumer {

	public static void main(String[] args) throws Exception {
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", "172.16.6.163:9092");
		kafkaProperties.put("group.id", "test-group3");
		kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> addSource = env
				.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), kafkaProperties));
		
		SingleOutputStreamOperator<SensorReading> sensorReadingStream = addSource
				.map(str -> new SensorReading(str.split(",")[0], Long.valueOf(str.split(",")[1]),
						Double.valueOf(str.split(",")[2])))
				.returns(SensorReading.class);
		
		sensorReadingStream.print("===>");

		env.execute("执行");
	}

}
