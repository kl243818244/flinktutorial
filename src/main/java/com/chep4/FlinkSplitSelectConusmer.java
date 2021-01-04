package com.chep4;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkSplitSelectConusmer {
	
	public static void main(String[] args) throws Exception {/*
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\STS\\STS4_WORKSPACES\\flinktutorial\\src\\main\\resources\\hello.txt");
		
		SingleOutputStreamOperator<SensorReading> returns = dataStreamSource.map(str -> new SensorReading(str.split(" ")[0],Long.valueOf(str.split(" ")[1]),Double.valueOf(str.split(" ")[2]))).returns(SensorReading.class);
		
		SplitStream<SensorReading> split = returns.split(sensorReading -> {
			List<String> collectors = new ArrayList<>();
			
			if(sensorReading.getTemperature() > 20) {
				collectors.add("high");
			}
			else {
				collectors.add("low");
			}
			
			return collectors;
		});
		
		DataStream<SensorReading> highSelect = split.select("high");
		
		highSelect.print("highSelect:");
		
		DataStream<SensorReading> lowSelect = split.select("low");
		
		lowSelect.print("lowSelect:");
		
		env.execute();
	*/}
	
}
