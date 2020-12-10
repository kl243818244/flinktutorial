package com.chep4;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 支持的基础类型
 * @author JayZhou
 *
 */
public class FlinkBasicTypesConsumer {
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// Long类型
		DataStreamSource<Long> fromElements = env.fromElements(1L,2L);
		
		SingleOutputStreamOperator<Long> map = fromElements.map(item -> item + 1).returns(Types.LONG);
		
		map.print("FlinkBasicTypesConsumer：");
		
		// 元祖类型 (Tuples)
		DataStreamSource<Tuple2<String, Integer>> tuple2Elements = env.fromElements(Tuple2.of("yahaha", 1),Tuple2.of("memeda", 2));
		
		tuple2Elements.print("tuple2Elements：");
		
		env.execute();
	}

}
