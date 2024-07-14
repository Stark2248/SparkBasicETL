package com.stark.spark.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SparkApplication {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:/Softwares/Hadoop/hadoop-2.7.3/");

		SpringApplication.run(SparkApplication.class, args);
	}

}
