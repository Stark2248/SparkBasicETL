package com.stark.spark.spark.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.avg;

@Component
public class AverageSales implements TransformService{
    @Override
    public Dataset<Row> transform(Dataset<Row> ds) {
        return ds.groupBy("article_id").agg(avg("price").alias("average"));
    }
}
