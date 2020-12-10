package com.chep4;

import java.util.Arrays;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Sensor {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<SensorReading> fromCollection = env.fromCollection(Arrays.asList(
				new SensorReading("sensor1", 1547718199L, 35.8), new SensorReading("sensor6", 1547718201L, 15.4),
				new SensorReading("sensor7", 1547718202L, 6.7), new SensorReading("sensor10", 1547718205L, 38.1)));

		fromCollection.print("fromCollection:").setParallelism(1);

		env.execute("执行流操作");
	}
}