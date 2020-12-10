package com.chep4;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 算子操作
 * @author JayZhou
 *
 */
public class FlinkAggregation {
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\STS\\STS4_WORKSPACES\\flinktutorial\\src\\main\\resources\\hello.txt");
		
		SingleOutputStreamOperator<SensorReading> mapReturns = dataStreamSource.map(str -> {
			String[] split = str.split(" ");
			SensorReading sensorReading = new SensorReading();
			sensorReading.setId(split[0]);
			sensorReading.setTimestamp(Long.valueOf(split[1]));
			sensorReading.setTemperature(Double.valueOf(split[2]));
			return sensorReading;
		}).returns(SensorReading.class);
		
		KeyedStream<SensorReading, Tuple> keyBy = mapReturns.keyBy("id");
		
		SingleOutputStreamOperator<SensorReading> reduce = keyBy.reduce((x,y)->{
			System.out.println("临时变量为:"+x);
			System.out.println("临时变量为:"+y);
			return new SensorReading(x.id,x.timestamp+1,y.temperature);
		});
		
		// 规约操作 和lambda表达式中的 reduce是一个概念
		reduce.print("reduce");
		
		env.execute();
	}

}
