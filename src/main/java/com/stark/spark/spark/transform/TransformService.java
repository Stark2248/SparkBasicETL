package com.stark.spark.spark.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

@Service
public interface TransformService {
    public Dataset<Row> transform(Dataset<Row> ds);
}
