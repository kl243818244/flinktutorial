package com.quota.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.types.Row;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyIndexRichSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> {
	private static final long serialVersionUID = -4512614186189764801L;

	private FlinkJedisConfigBase flinkJedisConfigBase;
	private RedisCommandsContainer redisCommandsContainer;
	
	private String indexName = "";
	
	public MyIndexRichSinkFunction(String indexName) {
		super();
		this.indexName = indexName;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.flinkJedisConfigBase = new FlinkJedisPoolConfig.Builder().setHost("172.16.6.163").setDatabase(5)
				.setPort(6379).build();
		try {
			this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
			this.redisCommandsContainer.open();
		} catch (Exception e) {
			log.error("Redis has not been properly initialized: ", e);
			throw e;
		}
	}

	@Override
	public void invoke(Tuple2<Boolean, Row> value) throws Exception {
		Boolean flag = value.f0;
		redisCommandsContainer.hset(this.indexName, "count", String.valueOf(value.f1.getField(0)));
		redisCommandsContainer.hset(this.indexName, "sum", String.valueOf(value.f1.getField(1)));
	}

	@Override
	public void close() throws Exception {
		if (redisCommandsContainer != null) {
			redisCommandsContainer.close();
		}
	}

}
