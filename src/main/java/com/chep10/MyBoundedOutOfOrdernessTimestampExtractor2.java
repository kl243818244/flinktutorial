package com.chep10;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.chep10.KafkaTable.Order;

public class MyBoundedOutOfOrdernessTimestampExtractor2 extends BoundedOutOfOrdernessTimestampExtractor<Order>{


	/**
	 * 
	 */
	private static final long serialVersionUID = -1157020373757689046L;

	public MyBoundedOutOfOrdernessTimestampExtractor2(Time maxOutOfOrderness) {
		super(maxOutOfOrderness);
		// TODO Auto-generated constructor stub
	}

	@Override
	public long extractTimestamp(Order element) {
		// TODO Auto-generated method stub
		return element.rowtime;
	}


}
