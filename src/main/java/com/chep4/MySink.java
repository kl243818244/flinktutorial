package com.chep4;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MySink extends RichSinkFunction<String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7048097432456478059L;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		// TODO Auto-generated method stub
		super.open(parameters);
	}
	
	/* 
	 * 实际运行业务逻辑的地方
	 */
	@Override
	public void invoke(String value, Context context) throws Exception {
		// TODO Auto-generated method stub
		super.invoke(value, context);
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}

}
