package com.chep4;

import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MyRedisMapper extends RedisSink<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4789855586549170347L;

	public MyRedisMapper(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<String> redisSinkMapper) {
		super(flinkJedisConfigBase, redisSinkMapper);
	}
	
}
