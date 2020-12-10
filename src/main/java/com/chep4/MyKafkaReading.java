package com.chep4;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MyKafkaReading {
	String id;
	Long timestamp;
	Double temperature;
}