package com.chep10;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.alibaba.fastjson.JSON;
 
public class KafkaTable {
 
	public static void main(String[] args) throws Exception {
 
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "172.16.6.163:9092");
		properties.put("group.id", "test-group3");
		FlinkKafkaConsumer011<String> consumer08 = new FlinkKafkaConsumer011<>("sensor", new SimpleStringSchema(), properties);
		
		
		DataStream<Order> raw = env.addSource(consumer08).map(new MapFunction<String, Order>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Order map(String s) throws Exception {
 
				if (s.contains("@")) {
					String[] split = s.split("@");
					Integer p1 = Integer.parseInt(split[0]);
					String p2 = split[1];
					Integer p3 = Integer.parseInt(split[2]);
					Long p4 = System.currentTimeMillis();
					return new Order(p1, p2, p3, p4);
				} else {
 
					Order order = JSON.parseObject(s, Order.class);
					if (order.rowtime == null)
						order.rowtime = System.currentTimeMillis();
					return order;
				}
			}
		}).assignTimestampsAndWatermarks(new MyBoundedOutOfOrdernessTimestampExtractor2(Time.seconds(2)));
 
		Table table = tEnv.fromDataStream(raw, "user,product,amount,rowtime.rowtime");
 
		tEnv.createTemporaryView("tOrder", table);
 
		Table table1 = tEnv.scan("tOrder")
			.window(Tumble.over("10.second").on("rowtime").as("w"))
			.groupBy("w,user,product")
			.select("user,product,amount.sum as sum_amount,w.start");
 
		String sql_tumble = "select user ,product,sum(amount) as sum_amount from tOrder group by TUMBLE(rowtime, INTERVAL '10' SECOND),user,product";
 
		String sql_hope = "select user ,product,sum(amount) as sum_amount from tOrder group by hop(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND),user,product";
 
		String sql_sesstion = "select user ,product,sum(amount) as sum_amount from tOrder group by session(rowtime, INTERVAL '12' SECOND),user,product";
 
		String sql_window_start = "select tumble_start(rowtime, INTERVAL '2' SECOND) as wStart,user ,product,sum(amount) as sum_amount from tOrder group by TUMBLE(rowtime, INTERVAL '2' SECOND),user,product";
 
		Table table2 = tEnv.sqlQuery(sql_window_start);
 
 
		DataStream<Tuple2<Boolean, Row>> resultStream = tEnv.toRetractStream(table2, Row.class);
		
		resultStream.print("==========>");
 
/*		resultStream.map(new MapFunction<Tuple2<Boolean, Result>, String>() {
			@Override
			public String map(Tuple2<Boolean, Result> tuple2) throws Exception {
				return "user:" + tuple2.f1.user + "  product:" + tuple2.f1.product + "   amount:" + tuple2.f1.sum_amount + "    wStart:" + tuple2.f1.wStart;
			}
		}).print();*/
		env.execute();
	}
 
 
	public static class Order {
		public Integer user;
		public String product;
		public int amount;
		public Long rowtime;
 
		public Order() {
			super();
		}
 
		public Order(Integer user, String product, int amount, Long rowtime) {
			this.user = user;
			this.product = product;
			this.amount = amount;
			this.rowtime = rowtime;
		}
	}
 
	public static class Result {
 
		public Integer user;
		public String product;
		public int sum_amount;
		public Timestamp wStart;
 
		public Result() {
 
		}

		@Override
		public String toString() {
			return "Result [user=" + user + ", product=" + product + ", sum_amount=" + sum_amount + ", wStart=" + wStart
					+ "]";
		}
		
	}
 
 
	public static class SessionTimeExtract implements AssignerWithPeriodicWatermarks<Order> {
 
		private final Long maxOutOfOrderness = 3500L;
		private Long currentMaxTimestamp = 0L;
		private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
 
 
		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
		}
 
		@Override
		public long extractTimestamp(Order order, long l) {
			long timestamp = order.rowtime;
			currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
			return timestamp;
		}
 
 
	}
 
 
}