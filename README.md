### Flink 特点

​	窗口函数

​	checkpiont 保证 exactly-once 有且只有一次 

​	流式计算 低延时、高吞吐、容扩展

​	能够处理乱序数据

​	支持有状态的计算

​	具有分层api 提供sqlapi 



### Flink中checkpoint执行流程

​	Flink的checkpoint机制原理来自“Chandy-Lamport algorithm”算法。 (分布式快照算)



### watermark

​	AssignerWithPunctuatedWatermarks：不间断的水印

​	AssignerWithPeriodicWatermarks：周期性水印



### Table Api 与 SQL

 	1. 加上 timestamp watermark
 	2. 对相应字段加上 rowtime

```java
package com.chep10;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
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
		String sql_window_start = "select id,count(id) from t_table t group by id , tumble(kafka_timestamp , interval '10' second) ";
		 
		Table table2 = tableStreamEnv.sqlQuery(sql_window_start);
		
		DataStream<Tuple2<Boolean, Row>> resultStream = tableStreamEnv.toRetractStream(table2, Row.class);
		
		resultStream.print("========>");
		
		streamEnv.execute("开始啦！！");
	}
}

```



```java
package com.chep10;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MyBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<KafkaReading>{


	/**
	 * 
	 */
	private static final long serialVersionUID = -1157020373757689046L;

	public MyBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
		super(maxOutOfOrderness);
		// TODO Auto-generated constructor stub
	}

	@Override
	public long extractTimestamp(KafkaReading element) {
		// TODO Auto-generated method stub
		return element.getKafka_timestamp();
	}


}

```

