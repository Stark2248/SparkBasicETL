package com.stark.spark.spark.load;

import com.stark.spark.spark.extract.SparkServiceImpl;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Writer implements WriterService{

    @Value("${LocationDir.Generating}")
    private String locationDirWrite;
    private static final Logger logger = LoggerFactory.getLogger(Writer.class);
    @Override
    public void write(Dataset<Row> ds) {
        logger.info("Writing.......");
        ds.write().mode("Overwrite").parquet(locationDirWrite);
        logger.info("Writing Done");
    }
}
