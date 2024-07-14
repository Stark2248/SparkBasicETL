package com.stark.spark.spark.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

@Service
public interface WriterService {
    public void write(Dataset<Row> ds);
}
