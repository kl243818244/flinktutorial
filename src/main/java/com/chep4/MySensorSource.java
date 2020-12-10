package com.chep4;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 
 * Flink 自定义source
 * 
 * @author JayZhou
 *
 */
public class MySensorSource implements SourceFunction<SensorReading> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1909339305032209839L;
	
	volatile Boolean running = true;
	

	@Override
	public void run(SourceContext<SensorReading> ctx) throws Exception {	
		SensorReading sensorReading = new SensorReading();
		Random random = new Random();
		sensorReading.setId(String.valueOf(random.nextInt(100)));
		sensorReading.setTemperature(Double.valueOf(random.nextInt(100)));
		sensorReading.setTimestamp(Long.valueOf(random.nextInt(100)));
		
		while(running) {
			ctx.collect(sensorReading);
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		this.running = false;
	}

}
