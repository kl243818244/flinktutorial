package com.chep10;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 周期性的水印
 * 
 * @author JayZhou
 *
 */
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
