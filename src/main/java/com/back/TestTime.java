package com.back;

import java.sql.Timestamp;

import org.apache.flink.table.shaded.org.joda.time.LocalDateTime;

public class TestTime {

	public static void main(String[] args) {
		
		
//		System.out.println(System.currentTimeMillis());
		
		
//		RFC3339_TIMESTAMP_FORMAT.parse("");
		
		LocalDateTime localDateTime = new LocalDateTime();
		
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		System.out.println(System.currentTimeMillis());
		
	}
	
}
