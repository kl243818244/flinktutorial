package com.quota;

import static org.apache.flink.table.api.Expressions.$;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.quota.entity.OdsClinicalHerbRecipe;
import com.quota.entity.OdsEncounterRegister;
import com.quota.entity.Settlement;
import com.quota.sink.MyIndexRichSinkFunction;
import com.quota.sink.MyRegisteredRichSinkFunction;

// 挂号人次
public class registeredPeopleIndex {

	public final static Integer REGISTERED_CONFIRM = 376749;
	
	public final static Integer SETTLEMENT = 399303919;
	
	public final static Integer HERB_RECIPE = 376582;

	public static void main(String[] args) throws Exception {
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(executionEnvironment, fsSettings);

		executionEnvironment.setParallelism(1);

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.6.161:9092");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testtttyyhy77");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		DataStream<String> kafkaStream = executionEnvironment.addSource(
				new FlinkKafkaConsumer<>("winning.dmts.queue.eventcollector", new SimpleStringSchema(), properties));

		// 挂号确认的流
		WatermarkStrategy<OdsEncounterRegister> odsEncounterRegisterWithTimestampAssigner = WatermarkStrategy
		        .<OdsEncounterRegister>forBoundedOutOfOrderness(Duration.ofSeconds(20))
		        .withTimestampAssigner((event, timestamp) -> event.getTs());
		
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
				, $("ts").rowtime()
				, $("RegisterId").as("REGISTER_ID"));
		
		// 收费笔数 收费金额
		WatermarkStrategy<Settlement> settlementWithTimestampAssigner = WatermarkStrategy
		        .<Settlement>forBoundedOutOfOrderness(Duration.ofSeconds(20))
		        .withTimestampAssigner((event, timestamp) -> event.getTs());
		
		SingleOutputStreamOperator<Settlement> settlementReturnsStream = kafkaStream
				.filter(new FilterFunction<String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(String value) throws Exception {
						JSONObject parseKafkaObject = JSONObject.parseObject(value);
						
						Integer type = parseKafkaObject.getJSONObject("input").getInteger("type");
						
						return type.equals(SETTLEMENT);
					}

				}).flatMap((String str, Collector<Settlement> out) -> {
					JSONObject filteredKafkaObject = JSONObject.parseObject(str);

					JSONArray settlement = filteredKafkaObject.getJSONObject("message")
							.getJSONArray("SETTLEMENT");

					List<Settlement> settlementList = settlement
							.toJavaList(Settlement.class);

					settlementList.forEach(x -> out.collect(x));
				}).returns(Settlement.class).assignTimestampsAndWatermarks(settlementWithTimestampAssigner);

		tableEnv.createTemporaryView("SETTLEMENT", settlementReturnsStream
				, $("ts").rowtime()
				, $("settlementSelfPayingAmount").as("SETTLEMENT_SELF_PAYING_AMOUNT"));
		
		
		// 处方张数 处方金额
		WatermarkStrategy<OdsClinicalHerbRecipe> herbRecipeWithTimestampAssigner = WatermarkStrategy
		        .<OdsClinicalHerbRecipe>forBoundedOutOfOrderness(Duration.ofSeconds(20))
		        .withTimestampAssigner((event, timestamp) -> event.getTs());
		
		SingleOutputStreamOperator<OdsClinicalHerbRecipe> herbRecipeReturnsStream = kafkaStream
				.filter(new FilterFunction<String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(String value) throws Exception {
						JSONObject parseKafkaObject = JSONObject.parseObject(value);
						
						Integer type = parseKafkaObject.getJSONObject("input").getInteger("type");
						
						return type.equals(HERB_RECIPE);
					}

				}).flatMap((String str, Collector<OdsClinicalHerbRecipe> out) -> {
					JSONObject filteredKafkaObject = JSONObject.parseObject(str);

					JSONArray settlement = filteredKafkaObject.getJSONObject("message")
							.getJSONArray("ODS_CLINICAL_HERB_RECIPE");

					List<OdsClinicalHerbRecipe> settlementList = settlement
							.toJavaList(OdsClinicalHerbRecipe.class);

					settlementList.forEach(x -> out.collect(x));
				}).returns(OdsClinicalHerbRecipe.class).assignTimestampsAndWatermarks(herbRecipeWithTimestampAssigner);

		tableEnv.createTemporaryView("ODS_CLINICAL_HERB_RECIPE", herbRecipeReturnsStream
				, $("ts").rowtime() , $("recipeAmount").as("RECIPE_AMOUNT") );
		

		// 挂号人次
		String registeredCount = " select count(*) from ODS_ENCOUNTER_REGISTER ";
		
		Table registeredCountSqlQuery = tableEnv.sqlQuery(registeredCount);
		
		DataStream<Tuple2<Boolean, Row>> registeredRetractStream = tableEnv.toRetractStream(registeredCountSqlQuery, Row.class);
		
		registeredRetractStream.print("registeredSql:==========>");
		
		registeredRetractStream.addSink(new MyRegisteredRichSinkFunction("registeredIndex"));
		
		// 收费笔数 收费金额
		String chargeSql = " select count(*),sum(SETTLEMENT_SELF_PAYING_AMOUNT) from SETTLEMENT ";
		
		Table chargeSqlQuery = tableEnv.sqlQuery(chargeSql);
		
		DataStream<Tuple2<Boolean, Row>> chargeRetractStream = tableEnv.toRetractStream(chargeSqlQuery, Row.class);
		
		chargeRetractStream.print("chargeSql:==========>");
		
		chargeRetractStream.addSink(new MyIndexRichSinkFunction("chargeIndex"));
		
		// 处方张数 处方金额
		String recipeSql = " select count(*),sum(RECIPE_AMOUNT) from ODS_CLINICAL_HERB_RECIPE ";
		
		Table recipeSqlQuery = tableEnv.sqlQuery(recipeSql);
		
		DataStream<Tuple2<Boolean, Row>> recipeRetractStream = tableEnv.toRetractStream(recipeSqlQuery, Row.class);
		
		recipeRetractStream.print("recipeSql:==========>");
		
		recipeRetractStream.addSink(new MyIndexRichSinkFunction("recipeIndex"));
		
		executionEnvironment.execute();
	}

}
