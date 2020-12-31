package com.utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import de.siegmar.fastcsv.reader.CsvContainer;
import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;

public class ReadCSVToKafka {
	
	/*public static void main(String[] args) {
		
	}
	
	public static void csvReadOperation() throws IOException {
		File file = new File("F:\\Excel\\customer-info.csv");
		CsvReader csvReader = new CsvReader();
		csvReader.setContainsHeader(true);

		CsvContainer csv = csvReader.read(file, StandardCharsets.UTF_8);
		for (CsvRow row : csv.getRows()) {
			System.out.println("First column of line: " + row.getField("用户编号"));
		}
	}

	public static void csvReadOperation1() throws IOException {
		File file = new File("F:\\Excel\\customer-info.csv");
		CsvReader csvReader = new CsvReader();

		CsvContainer csv = csvReader.read(file, StandardCharsets.UTF_8);
		for (CsvRow row : csv.getRows()) {
			System.out.println("Read line: " + row);
			System.out.println("First column of line: " + row.getField(0));
			if (row.getOriginalLineNumber() != 1) {
				ResultInfo resultInfo = new ResultInfo();
				resultInfo.setUserId(row.getField(0));
				resultInfo.setUserPhone(row.getField(1));
				resultInfo.setUserName(row.getField(2));
				resultInfo.setRegistTime(row.getField(3));
				resultInfo.setUserlevel(row.getField(4));
				resultInfo.setAppName(row.getField(10));
				listResultInfo.add(resultInfo);
			}

		}
	}

	public static void csvReadOperation2() throws IOException {
		File file = new File("F:\\Excel\\customer-info.csv");
		CsvReader csvReader = new CsvReader();

		try (CsvParser csvParser = csvReader.parse(file, StandardCharsets.UTF_8)) {
			CsvRow row;
			while ((row = csvParser.nextRow()) != null) {
				System.out.println("Read line: " + row);
				System.out.println("First column of line: " + row.getField(0));
			}
		}
	}*/
}
