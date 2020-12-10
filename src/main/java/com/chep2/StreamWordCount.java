package com.chep2;

import java.util.Arrays;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 
 * 流的方式
 * 
 * @author JayZhou
 *
 */
public class StreamWordCount {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> textStream = env.socketTextStream("", 12);

		textStream
				.flatMap((String a, Collector<String> out) -> Arrays.stream(a.split(" ")).forEach(x -> out.collect(x)))
				.returns(Types.STRING).filter(str -> !str.isEmpty()).map(str -> new Tuple2<String, Integer>(str, 1))
				.returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).sum(1).print();
		
		env.execute("类似于监听程序");
	}

}
