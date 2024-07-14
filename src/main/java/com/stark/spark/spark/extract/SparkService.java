package com.stark.spark.spark.extract;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

@Service
public interface SparkService {
    Dataset<Row> process(String fileLocation);
}
