package com.chep4;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
	String id;
	Long timestamp;
	Double temperature;
}