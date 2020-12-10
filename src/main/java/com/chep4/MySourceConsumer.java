package com.chep4;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySourceConsumer {
	
	public static void main(String[] args) throws Exception {
		MySensorSource mySensorSource = new MySensorSource();
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStreamSource<SensorReading> addSource = env.addSource(mySensorSource);
		
		addSource.print("mySensorSource:");
		
		env.execute("执行自定义stream流");
	}

}
