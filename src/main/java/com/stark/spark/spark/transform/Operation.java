package com.stark.spark.spark.transform;

import org.apache.spark.internal.config.R;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

@Component
public class Operation implements TransformService{
    public Dataset<Row> transform(Dataset<Row> ds){
        Dataset<Row> transformedDs = ds.withColumn("customer_id",ds.col("customer_id").cast(DataTypes.StringType));
        //transformedDs.printSchema();
        transformedDs = transformedDs.withColumn("customer_id", functions.lpad(functions.expr("customer_id"), 4, "0"));
        return transformedDs;
    }

    public Dataset<Row> getMaxPricePerWeek(Dataset<Row> ds){

//        WindowSpec windowSpec = Window.partitionBy("week").orderBy(functions.desc("price"));
//        Dataset<Row> rankedDs = ds.withColumn("rank", rank().over(windowSpec));
//        rankedDs.filter(col("rank").equalTo(1))
//                .select("yearday","week", "customer_id", "article_id", "price").show();
//        Dataset<Row> maxPrice = ds.groupBy("week").agg(functions.max("price").alias("Max_Price"));
//        //ds.select("yearday","week","customer_id","article_id","price").where(col("price").isInCollection(maxPrice.select("Max_Price").collectAsList())).show();
//
//        ds.join(maxPrice, ds.col("week").equalTo(maxPrice.col("week"))
//                        .and(ds.col("price").equalTo(maxPrice.col("Max_Price"))))
//                .select(ds.col("yearday"), ds.col("week"), ds.col("customer_id"), ds.col("article_id"), ds.col("price")).show();

        Dataset<Row> maxPrice = ds.groupBy("week").agg(max("price").alias("Max_Price"));

// Alias the original dataset and the max price dataset
        Dataset<Row> dsAlias = ds.as("ds");
        Dataset<Row> maxPriceAlias = maxPrice.as("maxPrice");

// Join the original dataset with the max price dataset
        return dsAlias.join(maxPriceAlias,
                        dsAlias.col("week").equalTo(maxPriceAlias.col("week"))
                                .and(dsAlias.col("price").equalTo(maxPriceAlias.col("Max_Price"))))
                .select(
                        dsAlias.col("yearday"),
                        dsAlias.col("week"),
                        dsAlias.col("customer_id"),
                        dsAlias.col("article_id"),
                        dsAlias.col("price")
                );
    }
}
