package com.chep4;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class FlinkKafkaProduce {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Tuple2<String, Integer>> dataStreamSource = env.fromElements(Tuple2.of("yahah", 1),
				Tuple2.of("memeda", 2));

		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", "172.16.6.163:9092");
		kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		SingleOutputStreamOperator<String> returns = dataStreamSource
				.map(item -> item.getField(0) + "-" + item.getField(1)).returns(Types.STRING);

		returns.addSink(new FlinkKafkaProducer011<String>("172.16.6.163:9092", "FlinkSinkConsumerTopic",
				new SimpleStringSchema()));

		env.execute();
	}

}