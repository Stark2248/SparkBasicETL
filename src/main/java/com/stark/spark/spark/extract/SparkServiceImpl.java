package com.stark.spark.spark.extract;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SparkServiceImpl implements SparkService {
    private static final Logger logger = LoggerFactory.getLogger(SparkServiceImpl.class);
    @Autowired
    SparkSession spark;

    @Override
    public Dataset<Row> process(String fileLocation){
        try {
            Dataset<Row> ds = spark.read().parquet(fileLocation);

            // Example: Perform data processing or show the dataset
            ds.show();

            return ds;

            // Example: Perform additional operations on 'ds'
            // ds.filter(...).groupBy(...).agg(...).show();

        } catch (Exception e) {
            // Handle or log the exception
            logger.error("Error processing file: {}", fileLocation, e);
            return null;
            // Throw the exception or handle it gracefully based on your application logic
        }
    }
}
