package com.chep10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class UserBehaviorKafkaConnectTest {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tEnv
            // 使用connect函数连接外部系统
            .connect(
                new Kafka()
                .version("universal")     // 必填，合法的参数有"0.8", "0.9", "0.10", "0.11"或"universal"
                .topic("sensor")   // 必填，Topic名
                .startFromLatest()        // 首次消费时数据读取的位置
//                .property("zookeeper.connect", "localhost:2181")  // Kafka连接参数
                .property("bootstrap.servers", "172.16.6.163:9092")
            )
            // 序列化方式 可以是JSON、Avro等
            .withFormat(new Json())
            // 数据的Schema
            .withSchema(
                new Schema()
                    .field("id", DataTypes.STRING())
                    .field("timestamp", DataTypes.TIMESTAMP(3))
                    .field("temperature", DataTypes.DOUBLE())
                    .rowtime(new Rowtime().timestampsFromField("timestamp").watermarksPeriodicAscending())
            )
            // 临时表的表名，后续可以在SQL语句中使用这个表名
            .createTemporaryTable("t_table");

        Table tumbleGroupByUserId = tEnv.sqlQuery("SELECT \n" +
                "\tid, \n" +
                "\tCOUNT(id) AS behavior_cnt, \n" +
                "\tTUMBLE_END(timestamp, INTERVAL '10' SECOND) AS end_ts \n" +
                "FROM t_table\n" +
                "GROUP BY user_id, TUMBLE(timestamp, INTERVAL '10' SECOND)");
        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(tumbleGroupByUserId, Row.class);
        result.print("============>");

        env.execute("table api");
    }
}
