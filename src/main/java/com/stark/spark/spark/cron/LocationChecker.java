package com.stark.spark.spark.cron;


import com.stark.spark.spark.extract.SparkService;
import com.stark.spark.spark.load.WriterService;
import com.stark.spark.spark.transform.AverageSales;
import com.stark.spark.spark.transform.Operation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

@Component
public class LocationChecker {

    private static final Logger logger = LoggerFactory.getLogger(LocationChecker.class);

    @Autowired
    SparkService sparkService;

    @Autowired
    AverageSales averageSalesService;

    @Autowired
    Operation operation;

    @Autowired
    WriterService writerService;

    @Value("${LocationDir.Incoming}")
    String locationDir;

    @Value("${LocationDir.Incoming.Processed}")
    String locationDirProcessed;

    @Scheduled(cron = "0 */1 * * * *")
    public void checkLocation(){
        logger.info("In Cron");
        //String locationDir = "D:/Java Personal Project/Spark Project ETL/Incoming/sales_cleaned.parquet";
        try {
            Dataset<Row> ds = sparkService.process(locationDir);
            Dataset<Row> averageDs = averageSalesService.transform(ds);
            averageDs.show();
            writerService.write(averageDs);
            Dataset<Row> transDsOp1 = operation.transform(ds);
            transDsOp1.show();
            Dataset<Row> transDsOp2 = operation.getMaxPricePerWeek(transDsOp1);
            transDsOp2.show();
            writerService.write(transDsOp2);
            logger.info("Processing completed for directory: {}", locationDir);
            move(locationDir);

        } catch (Exception e) {
            logger.error("Error processing directory {}: {}", locationDir, e.getMessage());
            // Handle or log the exception accordingly
        }

    }

    private void move(String location){
        Path source = Paths.get(location);
        Path destination = Paths.get(locationDirProcessed);
        Path destinationPath = destination.resolve("sales_cleaned.parquet");
        try {
            // Create the destination directory if it doesn't exist
            if (Files.notExists(destination)) {
                Files.createDirectories(destination);
            }

            if (Files.exists(destinationPath)) {
                // Rename the existing file with a timestamp
                String existingFullFileName = destinationPath.getFileName().toString();
                String existingFileName = existingFullFileName.split("\\.")[0];
                String renamedFileName = existingFileName+ "_" + System.currentTimeMillis()+".parquet";
                Path renamedPath = destination.resolve(renamedFileName);
                Files.move(destinationPath, renamedPath, StandardCopyOption.REPLACE_EXISTING);
            }else{
                Files.move(source, destinationPath, StandardCopyOption.REPLACE_EXISTING);
            }
            // Move the source file to the destination
            logger.info("File moved successfully.");
        } catch (IOException e) {
            logger.error("Error moving the file: " + e.getMessage());
        }
    }
}
