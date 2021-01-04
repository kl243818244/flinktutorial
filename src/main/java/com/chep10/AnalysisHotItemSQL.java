package com.chep10;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class AnalysisHotItemSQL {
	
	public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", "172.16.6.163:9092");
		kafkaProperties.put("group.id", "test-group3");
		kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		DataStreamSource<String> addSource = env
				.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), kafkaProperties));

		
        DataStream<UserBehavior> dataStream = addSource.map(new MapFunction<String, UserBehavior>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
            public UserBehavior map(String s) throws Exception {
                String[] dataArray = s.split(",");
                return new UserBehavior(Long.parseLong(dataArray[0]),Long.parseLong(dataArray[1]),Integer.parseInt(dataArray[2]),dataArray[3],Long.parseLong(dataArray[4]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(1)) {
            /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
            public long extractTimestamp(UserBehavior element) {
                return element.getTimestamp()*1000L;
            }
        });

        // ---------------  开始区 ---------------------------------------------
        
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // 创建临时表
        tableEnv.createTemporaryView("UserBehavior", dataStream,"itemId,behavior,timestamp.rowtime as ts");

        String sql = ("select * from (select * from(select itemId, count(itemId) as cnt " +
                "UserBehavior where behavior = 'pv' group by itemId )) ").trim();

        Table topNResultTable = tableEnv.sqlQuery(sql);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(topNResultTable, Row.class);
        tuple2DataStream.print();




        // --------------- 结束区 ------------------------------------------------
        env.execute("Top PV");
    }


}
