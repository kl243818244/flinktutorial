package com.chep6;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * 时间窗口 => 滚动 、 滑动、session
 * @author JayZhou
 *
 */
public class TimeWindowConsumer {
	
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", "172.16.6.163:9092");
		kafkaProperties.put("group.id", "test-group3");
		kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		DataStreamSource<String> addSource = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), kafkaProperties));
		
		SingleOutputStreamOperator<Integer> returns = addSource.flatMap((String str,Collector<Integer> out) -> out.collect(Integer.valueOf(str.split(",")[0]))).returns(Integer.class);
		
		
		// WindowFunction
//		returns.windowAll(assigner)
		
		// 时间窗口
//		returns.timeWindowAll(Time.seconds(10),Time.seconds(2)).sum(0).print("sum：");
		
		// 计数窗口 | 滑动窗口 =》 ( integer : 接收的个数 ,  slide_size : 传入key的相同数量  )
//		returns.countWindowAll(size)
		
		env.execute();
	}
	

}
