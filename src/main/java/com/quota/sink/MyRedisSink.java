package com.quota.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.types.Row;

public class MyRedisSink implements RedisMapper<Tuple2<Boolean, Row>> {
	private static final long serialVersionUID = -2554693635275704261L;

	@Override
	public RedisCommandDescription getCommandDescription() {
		return new RedisCommandDescription(RedisCommand.HSET,"testkey");
	}

	@Override
	public String getKeyFromData(Tuple2<Boolean, Row> data) {
		return String.valueOf(data.f1.getField(0)) ;
	}

	@Override
	public String getValueFromData(Tuple2<Boolean, Row> data) {
		return String.valueOf(String.valueOf(data.f1.getField(1)))  ;
	}

}
