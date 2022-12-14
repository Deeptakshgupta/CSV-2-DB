package com.demo;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class CsvToDb2Application {

	public static void main(String[] args) {
		SpringApplication.run(CsvToDb2Application.class, args);
	}

}
