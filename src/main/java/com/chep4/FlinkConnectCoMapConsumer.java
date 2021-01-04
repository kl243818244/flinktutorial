package com.chep4;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 
 * split 过时 需要用到 Side-outputs函数
 * @author JayZhou
 *
 */
public class FlinkConnectCoMapConsumer {

	public static void main(String[] args) throws Exception {/*

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> dataStreamSource = env
				.readTextFile("D:\\STS\\STS4_WORKSPACES\\flinktutorial\\src\\main\\resources\\hello.txt");

		SingleOutputStreamOperator<SensorReading> returns = dataStreamSource
				.map(str -> new SensorReading(str.split(" ")[0], Long.valueOf(str.split(" ")[1]),
						Double.valueOf(str.split(" ")[2])))
				.returns(SensorReading.class);

		SplitStream<SensorReading> split = returns.split(sensorReading -> {
			List<String> collectors = new ArrayList<>();

			if (sensorReading.getTemperature() > 20) {
				collectors.add("high");
			} else {
				collectors.add("low");
			}

			return collectors;
		});

		// 拆分成两个流
		DataStream<SensorReading> highSelect = split.select("high");

		DataStream<SensorReading> lowSelect = split.select("low");

		ConnectedStreams<SensorReading, SensorReading> connect = highSelect.connect(lowSelect);

		SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<SensorReading, SensorReading, String>() {
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 7837809137169606260L;

			@Override
			public String map1(SensorReading value) throws Exception {
				return value.getId();
			}

			@Override
			public String map2(SensorReading value) throws Exception {
				return value.getId();
			}
		}).returns(Types.STRING);

		map.print("FlinkConnectCoMapConsumer：");
		
		env.execute();
	*/}

}
