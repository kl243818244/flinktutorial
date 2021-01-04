package com.quota;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.commerce.userbehavioranalysis.CSVReaderWithSQL.UserBehavior;
import com.quota.entity.OdsEncounterRegister;
import static org.apache.flink.table.api.Expressions.*;

// 挂号人次
public class registeredPeopleIndex {

	public final static Integer REGISTERED_CONFIRM = 376749;

	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment, fsSettings);

		executionEnvironment.setParallelism(1);

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.6.161:9092");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testxsdddx");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		DataStream<String> kafkaStream = executionEnvironment.addSource(
				new FlinkKafkaConsumer<>("winning.dmts.queue.eventcollector", new SimpleStringSchema(), properties));

		// 挂号确认的流
		WatermarkStrategy<OdsEncounterRegister> odsEncounterRegisterWithTimestampAssigner = WatermarkStrategy
		        .<OdsEncounterRegister>forBoundedOutOfOrderness(Duration.ofSeconds(20))
		        .withTimestampAssigner((event, timestamp) -> event.getRegisteredAt().getTime());
		
		
		SingleOutputStreamOperator<OdsEncounterRegister> odsEncounterRegisterReturnsStream = kafkaStream
				.filter(new FilterFunction<String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(String value) throws Exception {
						JSONObject parseKafkaObject = JSONObject.parseObject(value);
						
						Integer type = parseKafkaObject.getJSONObject("input").getInteger("type");
						
						return type.equals(REGISTERED_CONFIRM);
					}

				}).flatMap((String str, Collector<OdsEncounterRegister> out) -> {
					JSONObject filteredKafkaObject = JSONObject.parseObject(str);

					JSONArray odsEncounterRegister = filteredKafkaObject.getJSONObject("message")
							.getJSONArray("ODS_ENCOUNTER_REGISTER");

					List<OdsEncounterRegister> odsEncounterRegisterList = odsEncounterRegister
							.toJavaList(OdsEncounterRegister.class);

					odsEncounterRegisterList.forEach(x -> out.collect(x));
				}).returns(OdsEncounterRegister.class).assignTimestampsAndWatermarks(odsEncounterRegisterWithTimestampAssigner);

		tableEnv.createTemporaryView("ODS_ENCOUNTER_REGISTER", odsEncounterRegisterReturnsStream
				, $("RegisterId").as("REGISTER_ID")
				, $("b"),
				$("rowtime").rowtime());// adds an event-time attribute named 'rowtime');

		// 挂号人次

		// 收费笔数 收费金额

		// 处方张数 处方金额

		executionEnvironment.execute();

//		registeredConfirmFilter.writeAsText("F:\\weining\\flink\\笔记课件\\1.笔记\\test.txt");
	}

}
