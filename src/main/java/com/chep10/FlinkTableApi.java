package com.chep10;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * TableApi
 * 
 * @author JayZhou
 *
 */
public class FlinkTableApi {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(1);
		streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", "172.16.6.163:9092");
		kafkaProperties.put("group.id", "test-group3");
		kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> addSource = env
				.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), kafkaProperties));

		
		DataStream<KafkaReading> sensorReadingStream = addSource
				.map(str -> {
					KafkaReading setTemperature = new KafkaReading();
					setTemperature.setId(Integer.valueOf(str.split("@")[0]));
					setTemperature.setKafka_timestamp(System.currentTimeMillis());
					setTemperature.setTemperature(Double.valueOf(str.split("@")[2]));
					
					return setTemperature;
				} )
				.returns(KafkaReading.class).assignTimestampsAndWatermarks(new MyBoundedOutOfOrdernessTimestampExtractor(Time.seconds(2)));
		
		
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

		StreamTableEnvironment tableStreamEnv = StreamTableEnvironment.create(streamEnv);
		
		tableStreamEnv.createTemporaryView("t_table", sensorReadingStream,"id,temperature,kafka_timestamp.rowtime");
		
//		Table fromDataStream = tableStreamEnv.fromDataStream(sensorReadingStream);

//		Table filter = fromDataStream.select(" id,temperature ").filter(" id= '1' ");

//		DataStream<Row> appendStream = tableStreamEnv.toAppendStream(filter, Row.class);

//		appendStream.print("streamTable:");

//		Table groupedTable = fromDataStream.groupBy(" id ").select(" id,id.count ");

//		DataStream<Tuple2<Boolean, Row>> retractStream = tableStreamEnv.toRetractStream(groupedTable, Row.class);

//		retractStream.print("retractStream:");
		
//		Table select = tableStreamEnv.from("t_table").window(Tumble.over("1.seconds").on("timestamp").as("tw")).groupBy("id,tw")
//		Table select = tableStreamEnv.from(" t_table ")
//				.window(Tumble.over(" 1000.millis ").on(" timestamp ").as(" total "))
//				.groupBy(" total,id ")
//				.select(" id ");
		
//		select.insertInto("spend_report");

//		DataStream<Tuple2<Boolean, Row>> retractStream = tableStreamEnv.toRetractStream(select, Row.class);

//		retractStream.print("windowStream:");
		
		
//		String sql_window_start = "select id from t_table group by tumble(timestamp, interval '2' second),id";
		String sql_window_start = "select id,count(id) from t_table t group by id , tumble(kafka_timestamp , interval '10' second)";
		 
		Table table2 = tableStreamEnv.sqlQuery(sql_window_start);
		
		DataStream<Tuple2<Boolean, Row>> resultStream = tableStreamEnv.toRetractStream(table2, Row.class);
		
		resultStream.print("========>");
		
		streamEnv.execute("开始啦！！");
	}
	
	

}
