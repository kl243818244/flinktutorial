package com.chep10;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaReading {
	Integer id;
	Double temperature;
	Long kafka_timestamp;
	
	
}