package com.sql;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class UDF {
	
	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		
		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		
		executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment, fsSettings);
		
		String ddl = "CREATE TABLE MyUserTable (\r\n" + 
				"  user_id BIGINT,\r\n" + 
				"  item_id BIGINT,\r\n" + 
				"  behavior STRING,\r\n" + 
				"  temperature INT,\r\n" + 
				"  app_time BIGINT,\r\n" + 
				"  ts AS TO_TIMESTAMP(FROM_UNIXTIME(app_time / 1000, 'yyyy-MM-dd HH:mm:ss')),\r\n" + 
				"  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\r\n" + 
				") WITH (\r\n" + 
				"  'connector.type' = 'kafka',\r\n" + 
				"  'connector.version' = '0.10',\r\n" + 
				"  'connector.topic' = 'sensor-json',\r\n" + 
				"  'connector.startup-mode' = 'earliest-offset',\r\n" + 
				"  'connector.properties.zookeeper.connect' = '172.16.6.163:2181',\r\n" + 
				"  'connector.properties.bootstrap.servers' = '172.16.6.163:9092',\r\n" + 
				"  'update-mode' = 'append',\r\n" + 
				"  'format.type' = 'json'\r\n" + 
				")";
		
		tableEnv.sqlUpdate(ddl);
		
		// 标量函数
		tableEnv.registerFunction("trans", new scalarFunc());
		
		tableEnv.registerFunction("tabletrans", new tableFunc());
		
		tableEnv.registerFunction("aggretrans", new aggreFunc());
		
		tableEnv.registerFunction("tableaggretrans", new TopTableAggre());
		
//		String sql = "select user_id , sum(user_id) , trans(user_id) , TUMBLE_START(ts, INTERVAL '5' second) as wStart from MyUserTable group by tumble( ts, interval '5' second ),user_id";
		
//		String sql = "select user_id , s , wStart from ( select user_id , sum(user_id) , trans(user_id) , TUMBLE_START(ts, INTERVAL '5' second) as wStart from MyUserTable group by tumble( ts, interval '5' second ),user_id ) ,  LATERAL TABLE(tabletrans(user_id)) as T(s)";
		
//		String sql = "select user_id , sum(user_id) , trans(user_id) , aggretrans(user_id) , TUMBLE_START(ts, INTERVAL '5' second) as wStart from MyUserTable group by tumble( ts, interval '5' second ),user_id";
		
		String sql = "select user_id , sum(user_id) , trans(user_id) , tableaggretrans(temperature)  , TUMBLE_START(ts, INTERVAL '5' second) as wStart from MyUserTable group by tumble( ts, interval '5' second ),user_id";
		
		Table sqlQuery = tableEnv.sqlQuery(sql);
		
		DataStream<Row> appendStream = tableEnv.toAppendStream(sqlQuery, Row.class);
		
		appendStream.print();
		
		
		
		
		
		executionEnvironment.execute("-----------start-----------");
	}
	
	/**
	 * 标量函数
	 * @author JayZhou
	 *
	 */
	public static class scalarFunc extends ScalarFunction{

		/**
		 * 
		 */
		private static final long serialVersionUID = -7943107087880113090L;
		
		public String eval(Long input) {
//			System.out.println("转换初始数据为:"+input);
			return input + "转换";
		}
	}
	
	public static class tableFunc extends TableFunction<String>{
		
		private static final long serialVersionUID = -1276443194043233746L;

		public void eval(Long str) {
			collect("这是第一行");
			collect("这是第二行");
		}
		
	}
	
	public static class aggreFunc extends AggregateFunction<Double, Map<String,Object>>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 6841291843592140435L;

		@Override
		public Double getValue(Map<String, Object> accumulator) {
			Long object = (Long) accumulator.get("sum");
			
			return Double.valueOf(object);
		}

		/* 初始化操作
		 */
		@Override
		public Map<String, Object> createAccumulator() {
			Map<String, Object> resultMap = new HashMap<String,Object>();
			
			resultMap.put("count", 0L);
			
			resultMap.put("sum", 0L);
			
			return resultMap;
		}
		
		/**
		 * 处理方法
		 * @param inputParam
		 * @param tmp
		 */
		public void accumulate(Map<String,Object> accumulator , Long inputParam) {
			Long count = (Long) accumulator.get("count");
			
			Long sum = (Long) accumulator.get("sum");
			
			sum += inputParam;
			count ++;
			
			accumulator.put("count", count);
			accumulator.put("sum", sum);
		}
		
	}
	
	public static class Top2Acc{
		Integer top1 = Integer.MIN_VALUE;
		Integer top2 = Integer.MIN_VALUE;
	}
	
	
	/**
	 * 通过聚合函数形成一张表
	 * 不能使用 SQL 这种方式
	 *
	 */
	public static class TopTableAggre extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Acc>{
		private static final long serialVersionUID = -3736041405507146817L;

		@Override
		public Top2Acc createAccumulator() {
			return null;
		}
		
		public void accumulate(Top2Acc accumulator , Integer inputParam) {
			if(inputParam > accumulator.top1) {
				accumulator.top2 = accumulator.top1;
				accumulator.top1 = inputParam;
			}
			else if( inputParam > accumulator.top2 ) {
				accumulator.top2 = inputParam;
			}
		}
		
		public void emitValue(Top2Acc accumulator , Collector<Tuple2<Integer, Integer>> out) {
			out.collect(Tuple2.of(accumulator.top1, 1));
			out.collect(Tuple2.of(accumulator.top2, 2));
		}
		
	}

}
