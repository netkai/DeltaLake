package com.temenos.analytics.deltalake;

import io.delta.tables.DeltaTable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.*;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.spark.sql.functions.*;


public class Lakehouse {
    private static final Logger logger = LogManager.getLogger(Lakehouse.class);
    private static final File FILE_LAKEHOUSE_ROOT = new File("C:\\TemenosWork\\2022\\Lakehouse");

    private static final AtomicLong LAST_TIME_MS = new AtomicLong();
    public static long uniqueCurrentTimeMS() {
        long now = System.currentTimeMillis();
        while(true) {
            long lastTime = LAST_TIME_MS.get();
            if (lastTime >= now)
                now = lastTime+1;
            if (LAST_TIME_MS.compareAndSet(lastTime, now))
                return now;
        }
    }

    public static void main(String[] args) {
        SparkConf sc = (new SparkConf())
                .setMaster("local[8]")
                .setAppName("Lakehouse")
                .set("spark.sql.ansi.enabled", "true")
                .set("spark.sql.storeAssignmentPolicy", "ANSI")
                .set("spark.sql.shuffle.partitions", "8")
                .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") // enabling DS v2
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");

        SparkSession spark = SparkSession.builder().config(sc).getOrCreate();

//        Dataset<Row> dfRate = spark.readStream()
//                .format("rate")
//                .option("numPartitions", 2)
//                .option("rowsPerSecond", 1)
//                .load();
//        try {
//            StreamingQuery rateQry = dfRate
////                    .withColumn("minute_at", minute(col("timestamp")))
////                    .select("minute_at")
//                    .writeStream()
//                    .format("console")
//                    .outputMode("update")
//                    .start();
//            rateQry.awaitTermination(30000);
////            rateQry.processAllAvailable();
//            rateQry.stop();
//        } catch (TimeoutException | StreamingQueryException ex) {
//            logger.error(ex.getMessage());
//        }
//
//        spark.stop();

        String lakehouseDir = FilenameUtils.separatorsToSystem(spark.conf().get("spark.sql.warehouse.dir"));
        lakehouseDir = lakehouseDir.replaceFirst("file:\\\\", "");

        spark.sql("SHOW DATABASES").show();
        spark.sql("SHOW TABLES").show(10);

        String tableName = "employee";

         // 1. CREATING SCHEMA (/DATABASE/NAMESPACE)
        String sqlDropDb = "DROP SCHEMA IF EXISTS demo";
        spark.sql(sqlDropDb);

        String sqlCreateDb = "CREATE SCHEMA IF NOT EXISTS demo COMMENT 'DATABASE and SCHEMA are the same thing' ";
        spark.sql(sqlCreateDb);
        spark.sql("SHOW SCHEMAS").show(false);

        // 2. DROPPING TABLE

        // 2.1 First, remove the metadata from Hive
        String sqlDrop = "DROP TABLE IF EXISTS demo." + tableName;
        spark.sql(sqlDrop);

        // 2.2 then remove the directory if is a path-based table, which is unmanaged/external

        /* Deleting directory in Windows. Different ways if on AWS, Azure, GCP */
        File tableDir = Paths.get(lakehouseDir, "demo.db", tableName).toFile();
        if (tableDir.exists()) {
            try {
                FileUtils.deleteDirectory(tableDir);
                logger.info("Directory and content deleted under: " + tableDir.toString());
            } catch (IOException e) {
                logger.error("Deleting directory issue: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }

        String sqlCreateTable = "CREATE OR REPLACE TABLE demo." + tableName +
                " (" +
                "BusinessEntityID INT NOT NULL, Title STRING, FirstName STRING, MiddleName STRING, " +
                "LastName STRING, Suffix STRING, JobTitle STRING, PhoneNumber STRING, " +
                "PhoneNumberType STRING, EmailAddress STRING, EmailPromotion STRING, " +
                "AddressLine1 STRING, AddressLine2 STRING, City STRING, StateProvinceName STRING," +
                "PostalCode STRING, CountryRegionName STRING" +
                " )" +
                " USING DELTA" +
                " PARTITIONED BY (StateProvinceName)";

        spark.sql(sqlCreateTable);

        String sqlInsert =
                "INSERT INTO demo." + tableName +
                        " (BusinessEntityID, Title, FirstName, MiddleName, LastName, Suffix, JobTitle, PhoneNumber, PhoneNumberType, EmailAddress, EmailPromotion, AddressLine1, AddressLine2, City, StateProvinceName, PostalCode, CountryRegionName) " +
                        "VALUES " +
                        "(1, NULL, 'Ken', 'J', 'Sánchez', NULL, 'Chief Executive Officer', '697-555-0142', 'Cell', 'ken0@adventure-works.com', 0, '4350 Minute Dr.', NULL, 'Newport Hills', 'Washington', '98006', 'United States')," +
                        "(2, NULL, 'Terri', 'Lee', 'Duffy', NULL, 'Vice President of Engineering', '819-555-0175', 'Work', 'terri0@adventure-works.com', 1, '7559 Worth Ct.', NULL, 'Renton', 'Washington', '98055', 'United States')," +
                        "(3, NULL, 'Roberto', NULL, 'Tamburello', NULL, 'Engineering Manager', '212-555-0187', 'Cell', 'roberto0@adventure-works.com', 0, '2137 Birchwood Dr', NULL, 'Redmond', 'Washington', '98052', 'United States')," +
                        "(4, NULL, 'Rob', NULL, 'Walters', NULL, 'Senior Tool Designer', '612-555-0100', 'Cell', 'rob0@adventure-works.com', 0, '5678 Lakeview Blvd.', NULL, 'Minneapolis', 'Minnesota', '55402', 'United States')," +
                        "(5, 'Ms.', 'Gail', 'A', 'Erickson', NULL, 'Design Engineer', '849-555-0139', 'Cell', 'gail0@adventure-works.com', 0, '9435 Breck Court', NULL, 'Bellevue', 'Washington', '98004', 'United States')," +
                        "(6, 'Mr.', 'Jossef', 'H', 'Goldberg', NULL, 'Design Engineer', '122-555-0189', 'Work', 'jossef0@adventure-works.com', 0, '5670 Bel Air Dr.', NULL, 'Renton', 'Washington', '98055', 'United States')," +
                        "(7, NULL, 'Dylan', 'A', 'Miller', NULL, 'Research and Development Manager', '181-555-0156', 'Work', 'dylan0@adventure-works.com', 2, '7048 Laurel', NULL, 'Kenmore', 'Washington', '98028', 'United States')," +
                        "(8, NULL, 'Diane', 'L', 'Margheim', NULL, 'Research and Development Engineer', '815-555-0138', 'Cell', 'diane1@adventure-works.com', 0, '475 Santa Maria', NULL, 'Everett', 'Washington', '98201', 'United States')," +
                        "(9, NULL, 'Gigi', '', 'Matthew', NULL, 'Research and Development Engineer', '185-555-0186', 'Cell', 'gigi0@adventure-works.com', 0, '7808 Brown St.', NULL, 'Bellevue', 'Washington', '98004', 'United States')," +
                        "(10, NULL, 'Michael', NULL, 'Raheem', NULL, 'Research and Development Manager', '330-555-2568', 'Work', 'michael6@adventure-works.com', 2, '1234 Seaside Way', NULL, 'San Francisco', 'California', '94109', 'United States')";

        spark.sql(sqlInsert);


        spark.sql("DROP TABLE IF EXISTS demo.Birthday");

        File path = new File(lakehouseDir, "demo.db\\Birthday");
        if (path.exists()) {
            try {
                FileUtils.deleteDirectory(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        String sqlCreateTableCalledBirthday = "CREATE TABLE demo.Birthday (" +
                "BusinessEntityID INT NOT NULL, " +
                "LoginID STRING, Birthday DATE, MaritalStatus STRING, Gender STRING, CurrentFlag TINYINT) " +
                "USING DELTA";
        spark.sql(sqlCreateTableCalledBirthday);
        sqlCreateTableCalledBirthday =
                "INSERT INTO demo.Birthday (" +
            "BusinessEntityID, LoginID, Birthday, MaritalStatus, Gender, CurrentFlag) " +
                "VALUES " +
                "(1, 'adventure-works\\ken0', CAST('1969-01-29' AS DATE), 'S', 'M', 1)" +
                ",(2, 'adventure-works\\terri0', CAST('1971-08-01' AS DATE), 'S', 'F', 1)" +
                ",(3, 'adventure-works\\roberto0', CAST('1974-11-12' AS DATE), 'M', 'M', 1)" +
                ",(4, 'adventure-works\\rob0', CAST('1974-12-23' AS DATE), 'S', 'M', 1)" +
                ",(5, 'adventure-works\\gail0', CAST('1952-09-27' AS DATE), 'M', 'F', 1)" +
                ",(6, 'adventure-works\\jossef0', CAST('1959-03-11' AS DATE), 'M', 'M', 1)" +
                ",(7, 'adventure-works\\dylan0', CAST('1987-02-24' AS DATE), 'M', 'M', 1)" +
                ",(8, 'adventure-works\\diane1', CAST('1986-06-05' AS DATE), 'S', 'F', 1)" +
                ",(9, 'adventure-works\\gigi0', CAST('1979-01-21' AS DATE), 'M', 'F', 1)" +
                ",(10, 'adventure-works\\michael6', CAST('1984-11-30' AS DATE), 'M', 'M', 1)" +
                ",(11, 'adventure-works\\ovidiu0', CAST('1978-01-17' AS DATE), 'S', 'M', 1)" +
                ",(12, 'adventure-works\\thierry0', CAST('1959-07-29' AS DATE), 'M', 'M', 1)" +
                ",(13, 'adventure-works\\janice0', CAST('1989-05-28' AS DATE), 'M', 'F', 1)" +
                ",(14, 'adventure-works\\michael8', CAST('1979-06-16' AS DATE), 'S', 'M', 1)" +
                ",(15, 'adventure-works\\sharon0', CAST('1961-05-02' AS DATE), 'M', 'F', 1)" +
                ",(16, 'adventure-works\\david0', CAST('1975-03-19' AS DATE), 'S', 'M', 1)" +
                ",(17, 'adventure-works\\kevin0', CAST('1987-05-03' AS DATE), 'S', 'M', 1)" +
                ",(18, 'adventure-works\\john5', CAST('1978-03-06' AS DATE), 'S', 'M', 1)" +
                ",(19, 'adventure-works\\mary2', CAST('1978-01-29' AS DATE), 'S', 'F', 1)" +
                ",(20, 'adventure-works\\wanida0', CAST('1975-03-17' AS DATE), 'M', 'F', 1)" +
                ",(21, 'adventure-works\\terry0', CAST('1986-02-04' AS DATE), 'M', 'M', 1)" +
                ",(22, 'adventure-works\\sariya0', CAST('1987-05-21' AS DATE), 'S', 'M', 1)" +
                ",(23, 'adventure-works\\mary0', CAST('1962-09-13' AS DATE), 'M', 'F', 1)" +
                ",(24, 'adventure-works\\jill0', CAST('1979-06-18' AS DATE), 'M', 'F', 1)" +
                ",(25, 'adventure-works\\james1', CAST('1983-01-07' AS DATE), 'S', 'M', 1)" +
                ",(26, 'adventure-works\\peter0', CAST('1982-11-03' AS DATE), 'M', 'M', 1)" +
                ",(27, 'adventure-works\\jo0', CAST('1956-10-08' AS DATE), 'S', 'F', 1)" +
                ",(28, 'adventure-works\\guy1', CAST('1988-03-13' AS DATE), 'M', 'M', 1)" +
                ",(29, 'adventure-works\\mark1', CAST('1979-09-25' AS DATE), 'S', 'M', 1)" +
                ",(30, 'adventure-works\\britta0', CAST('1989-09-28' AS DATE), 'M', 'F', 1)";
        spark.sql(sqlCreateTableCalledBirthday);

        spark.sql("SELECT * FROM demo.Birthday ORDER BY BusinessEntityID").show(false);

        spark.sql("SHOW TABLES IN demo").show(10);

        // Using native sql expression
        spark.sql("select b.Birthday, e.* from demo.employee e inner join demo.birthday b on e.BusinessEntityID == b.BusinessEntityID order by e.BusinessEntityID").show(false);

        // Using temporary view created after dataframe
        Dataset<Row> dfEmp = spark.sql("select * from demo.employee");
        dfEmp.createOrReplaceTempView("E");
        Dataset<Row> dfBDay = spark.sql("select Birthday, BusinessEntityID from demo.birthday");
        dfBDay.createOrReplaceTempView("B");
        spark.sql("select * from E inner join B on E.BusinessEntityID == B.BusinessEntityID order by E.BusinessEntityID")
             .show(30);

        // Using api
        dfEmp.as("e")
            .join(dfBDay.withColumn("Age", (months_between(current_date(), col("Birthday"), true).divide(12).cast("INT")))
                        .as("b"),
                  dfEmp.col("BusinessEntityID").equalTo(dfBDay.col("BusinessEntityID")),
                  "inner")
            .select("b.BusinessEntityID", "b.Birthday", "b.Age", "e.*")
            .where(dfEmp.col("BusinessEntityID").leq(10))
//                .sort(dfBDay.col("Birthday").desc())
            .orderBy(desc("b.Age"))
            .show(false);

        // Delete all
        spark.sql("delete from demo.employee");

        // Read from Stream

        File fileCheckpoint = new File(FILE_LAKEHOUSE_ROOT, "_checkpoints");
        File fileOutput = new File(FILE_LAKEHOUSE_ROOT, "Output");
        File fileCsv = new File(fileOutput, "Csv");
        File fileJson = new File(fileOutput, "Json");
        File fileParquet = new File(fileOutput, "Parquet");

        // Clean up Output directory
        if (fileOutput.exists()) {
            try {
                FileUtils.cleanDirectory(fileOutput);
            } catch (IOException ex) {
                logger.error(ex.getMessage());
            }
        }

        // Clean up checkpoint directory
        if (fileCheckpoint.exists()) {
            try {
                FileUtils.cleanDirectory(fileCheckpoint);
            } catch (IOException ex) {
                logger.error(ex.getMessage());
            }
        }

        StreamingQuery streamingQuery = null;

        try {
            Dataset<Row> stream = spark.readStream()
                    .format("delta")
//                    .option("ignoreDeletes", true)
                    .option("ignoreChanges", true)
                    .table("demo." + tableName);

            streamingQuery = stream.writeStream()
                     .outputMode("append")
                     .format("console")
                     .option("checkpointLocation", fileCheckpoint.getCanonicalPath())
//                     .start();

//                      .option("txnVersion", 9)  // uniqueCurrentTimeMS())
//                      .option("startingVersion", 0)

//                    /* idempotent writes when using foreachBatch ?? */
                    .foreachBatch((Dataset<Row> dataFrame, Long batchId) -> {
                                dataFrame.write()
                                        .format("parquet")
                                        .mode(SaveMode.Append)
                                        .save((new File(fileParquet, tableName)).getCanonicalPath());
                                dataFrame.write()
                                        .format("json")
                                        .option("ignoreNullFields", false)
                                        .mode(SaveMode.Append)  // To try other modes such as Overwrite and Ignore...
                                        .save((new File(fileJson, tableName)).getCanonicalPath());
                                dataFrame.write()
                                        .format("csv")
                                        .option("header", true)
                                        .option("locale", "en-US")
                                        .option("encoding", "UTF-8")
                                        .mode(SaveMode.Append)
                                        .save((new File(fileCsv, tableName)).getCanonicalPath());
                                logger.info("BatchId = " + batchId + "; AppId = " + spark.sparkContext().applicationId());
                            }
                    ).start();

            streamingQuery.awaitTermination(20000);

        } catch (IOException | StreamingQueryException | TimeoutException ex) {
            logger.error(ex.getMessage());
            throw new RuntimeException(ex);
        }


        sqlInsert =
         "INSERT INTO demo." + tableName +
           " (BusinessEntityID, Title, FirstName, MiddleName, LastName, Suffix, JobTitle, PhoneNumber, PhoneNumberType, EmailAddress, EmailPromotion, AddressLine1, AddressLine2, City, StateProvinceName, PostalCode, CountryRegionName) " +
         "VALUES " +
            "(1, NULL, 'Ken', 'J', 'Sánchez', NULL, 'Chief Executive Officer', '697-555-0142', 'Cell', 'ken0@adventure-works.com', 0, '4350 Minute Dr.', NULL, 'Newport Hills', 'Washington', '98006', 'United States')," +
            "(2, NULL, 'Terri', 'Lee', 'Duffy', NULL, 'Vice President of Engineering', '819-555-0175', 'Work', 'terri0@adventure-works.com', 1, '7559 Worth Ct.', NULL, 'Renton', 'Washington', '98055', 'United States')," +
            "(3, NULL, 'Roberto', NULL, 'Tamburello', NULL, 'Engineering Manager', '212-555-0187', 'Cell', 'roberto0@adventure-works.com', 0, '2137 Birchwood Dr', NULL, 'Redmond', 'Washington', '98052', 'United States')," +
            "(4, NULL, 'Rob', NULL, 'Walters', NULL, 'Senior Tool Designer', '612-555-0100', 'Cell', 'rob0@adventure-works.com', 0, '5678 Lakeview Blvd.', NULL, 'Minneapolis', 'Minnesota', '55402', 'United States')," +
            "(5, 'Ms.', 'Gail', 'A', 'Erickson', NULL, 'Design Engineer', '849-555-0139', 'Cell', 'gail0@adventure-works.com', 0, '9435 Breck Court', NULL, 'Bellevue', 'Washington', '98004', 'United States')," +
            "(6, 'Mr.', 'Jossef', 'H', 'Goldberg', NULL, 'Design Engineer', '122-555-0189', 'Work', 'jossef0@adventure-works.com', 0, '5670 Bel Air Dr.', NULL, 'Renton', 'Washington', '98055', 'United States')," +
            "(7, NULL, 'Dylan', 'A', 'Miller', NULL, 'Research and Development Manager', '181-555-0156', 'Work', 'dylan0@adventure-works.com', 2, '7048 Laurel', NULL, 'Kenmore', 'Washington', '98028', 'United States')," +
            "(8, NULL, 'Diane', 'L', 'Margheim', NULL, 'Research and Development Engineer', '815-555-0138', 'Cell', 'diane1@adventure-works.com', 0, '475 Santa Maria', NULL, 'Everett', 'Washington', '98201', 'United States')," +
            "(9, NULL, 'Gigi', '', 'Matthew', NULL, 'Research and Development Engineer', '185-555-0186', 'Cell', 'gigi0@adventure-works.com', 0, '7808 Brown St.', NULL, 'Bellevue', 'Washington', '98004', 'United States')," +
            "(10, NULL, 'Michael', NULL, 'Raheem', NULL, 'Research and Development Manager', '330-555-2568', 'Work', 'michael6@adventure-works.com', 2, '1234 Seaside Way', NULL, 'San Francisco', 'California', '94109', 'United States')";

        spark.sql(sqlInsert);
        spark.sql("SELECT * FROM demo." + tableName).show(10);  // spark-delta sql
        spark.sql("DESCRIBE HISTORY demo." + tableName).show();
        DeltaTable.forName(spark, "demo." + tableName)  // java api
                .toDF()
                .select("BusinessEntityID", "Title", "FirstName")
                .show();

        spark.sql("SELECT * FROM demo." + tableName + " WHERE StateProvinceName = 'Minnesota' ORDER BY BusinessEntityID").show(false);
//        spark.sql("DELETE FROM demo." + tableName + " WHERE StateProvinceName = 'Minnesota'");
        spark.sql("DELETE FROM demo." + tableName + " WHERE BusinessEntityID = 4");
        spark.sql("UPDATE demo." + tableName + " SET MiddleName = 'Alice' WHERE BusinessEntityID = 5");
        spark.sql("SELECT * FROM demo." + tableName + " ORDER BY BusinessEntityID").show(false);

        // Watch for the stream append mode
        sqlInsert =
                "INSERT INTO demo." + tableName +
                        " (BusinessEntityID, Title, FirstName, MiddleName, LastName, Suffix, JobTitle, PhoneNumber, PhoneNumberType, EmailAddress, EmailPromotion, AddressLine1, AddressLine2, City, StateProvinceName, PostalCode, CountryRegionName) " +
                        "VALUES " +
                        "(11, NULL, 'Ken', 'J', 'Sánchez', NULL, 'Chief Executive Officer', '697-555-0142', 'Cell', 'ken0@adventure-works.com', 0, '4350 Minute Dr.', NULL, 'Newport Hills', 'Washington', '98006', 'United States')," +
                        "(12, NULL, 'Terri', 'Lee', 'Duffy', NULL, 'Vice President of Engineering', '819-555-0175', 'Work', 'terri0@adventure-works.com', 1, '7559 Worth Ct.', NULL, 'Renton', 'Washington', '98055', 'United States')," +
                        "(13, NULL, 'Roberto', NULL, 'Tamburello', NULL, 'Engineering Manager', '212-555-0187', 'Cell', 'roberto0@adventure-works.com', 0, '2137 Birchwood Dr', NULL, 'Redmond', 'Washington', '98052', 'United States')," +
                        "(14, NULL, 'Rob', NULL, 'Walters', NULL, 'Senior Tool Designer', '612-555-0100', 'Cell', 'rob0@adventure-works.com', 0, '5678 Lakeview Blvd.', NULL, 'Minneapolis', 'Minnesota', '55402', 'United States')," +
                        "(15, 'Ms.', 'Gail', 'A', 'Erickson', NULL, 'Design Engineer', '849-555-0139', 'Cell', 'gail0@adventure-works.com', 0, '9435 Breck Court', NULL, 'Bellevue', 'Washington', '98004', 'United States')," +
                        "(16, 'Mr.', 'Jossef', 'H', 'Goldberg', NULL, 'Design Engineer', '122-555-0189', 'Work', 'jossef0@adventure-works.com', 0, '5670 Bel Air Dr.', NULL, 'Renton', 'Washington', '98055', 'United States')," +
                        "(17, NULL, 'Dylan', 'A', 'Miller', NULL, 'Research and Development Manager', '181-555-0156', 'Work', 'dylan0@adventure-works.com', 2, '7048 Laurel', NULL, 'Kenmore', 'Washington', '98028', 'United States')," +
                        "(18, NULL, 'Diane', 'L', 'Margheim', NULL, 'Research and Development Engineer', '815-555-0138', 'Cell', 'diane1@adventure-works.com', 0, '475 Santa Maria', NULL, 'Everett', 'Washington', '98201', 'United States')," +
                        "(19, NULL, 'Gigi', '', 'Matthew', NULL, 'Research and Development Engineer', '185-555-0186', 'Cell', 'gigi0@adventure-works.com', 0, '7808 Brown St.', NULL, 'Bellevue', 'Washington', '98004', 'United States')," +
                        "(20, NULL, 'Michael', NULL, 'Raheem', NULL, 'Research and Development Manager', '330-555-2568', 'Work', 'michael6@adventure-works.com', 2, '1234 Seaside Way', NULL, 'San Francisco', 'California', '94109', 'United States')";

        spark.sql(sqlInsert);

        sqlInsert =
                "INSERT INTO demo." + tableName +
                        " (BusinessEntityID, Title, FirstName, MiddleName, LastName, Suffix, JobTitle, PhoneNumber, PhoneNumberType, EmailAddress, EmailPromotion, AddressLine1, AddressLine2, City, StateProvinceName, PostalCode, CountryRegionName) " +
                        "VALUES " +
                        "(21, NULL, 'Ken', 'J', 'Sánchez', NULL, 'Chief Executive Officer', '697-555-0142', 'Cell', 'ken0@adventure-works.com', 0, '4350 Minute Dr.', NULL, 'Newport Hills', 'Washington', '98006', 'United States')," +
                        "(22, NULL, 'Terri', 'Lee', 'Duffy', NULL, 'Vice President of Engineering', '819-555-0175', 'Work', 'terri0@adventure-works.com', 1, '7559 Worth Ct.', NULL, 'Renton', 'Washington', '98055', 'United States')";

        spark.sql(sqlInsert);

        spark.sql("SELECT * FROM demo." + tableName + " ORDER BY BusinessEntityID").show(100);  // spark-delta sql
        spark.sql("DESCRIBE HISTORY demo." + tableName).show();

        if (streamingQuery != null) {
            try {
                streamingQuery.processAllAvailable();
                streamingQuery.stop();
            } catch (TimeoutException ex) {

            }
        }

//        // Clean up Output directory
//
//        if (fileOutput.exists()) {
//            try {
//                FileUtils.cleanDirectory(fileOutput);
//            } catch (IOException ex) {
//                logger.error(ex.getMessage());
//            }
//        }
//
//        // Clean up checkpoint directory
//        if (fileCheckpoint.exists()) {
//            try {
//                FileUtils.cleanDirectory(fileCheckpoint);
//            } catch (IOException ex) {
//                logger.error(ex.getMessage());
//            }
//        }

        spark.stop();

       /**********************************************************************************/



//        // UPSERT - batch
//        spark.sql("DROP TABLE IF EXISTS demo.merge_src");
//        spark.sql("DROP TABLE IF EXISTS demo.merge_tgt");
//
//        spark.sql("CREATE OR REPLACE TABLE demo.merge_tgt (ID INT, NAME STRING, QTY INT) USING DELTA");
//        spark.sql("INSERT INTO demo.merge_tgt (ID, NAME, QTY) VALUES (1, 'A', 1), (2, 'B', 2), (3, 'C', 3)");
//        spark.sql("SELECT * FROM demo.merge_tgt ORDER BY ID").show();
//
//        spark.sql("CREATE OR REPLACE TABLE demo.merge_src (ID INT, NAME STRING, QTY INT) USING DELTA");
//        spark.sql("INSERT INTO demo.merge_src (ID, NAME, QTY) VALUES (1, 'AA', 11), (3, 'C-deleted', 3), (4, 'D', 4), (5, 'E', 5)");
//        spark.sql("SELECT * FROM demo.merge_src ORDER BY ID").show();
//
//        // can't do DELETE on NOT MATCHED condition
//        String sqlMerge =
//                "MERGE INTO demo.merge_tgt tgt " +
//                "USING demo.merge_src src " +
//                "ON tgt.ID = src.ID " +
//                "WHEN MATCHED AND src.NAME RLIKE '.*-deleted\\s*$' THEN DELETE " +  /* can use LIKE but no ILIKE as of 2022-08-18 */
//                "WHEN MATCHED THEN UPDATE SET NAME = src.NAME, QTY = src.QTY " +
//                "WHEN NOT MATCHED THEN INSERT (ID, NAME, QTY) VALUES (src.ID, src.NAME, src.QTY)";
//        spark.sql(sqlMerge);
//        spark.sql("SELECT * FROM demo.merge_tgt ORDER BY ID").show();
//
//        // Enabling schema evolution on merge
//        spark.sql("DELETE FROM demo.merge_tgt");
//        spark.sql("INSERT INTO demo.merge_tgt (ID, NAME, QTY) VALUES (1, 'A', 1), (2, 'B', 2), (3, 'C', 3)");
//        spark.sql("DROP TABLE IF EXISTS demo.merge_src");
//        spark.sql("CREATE OR REPLACE TABLE demo.merge_src (ID INT, ALIAS STRING, QTY INT) USING DELTA");
//        spark.sql("INSERT INTO demo.merge_src (ID, ALIAS, QTY) VALUES (1, 'Ace', 11), (3, 'Si', 3), (4, 'Dee', 4)");
//        spark.conf().set("spark.databricks.delta.schema.autoMerge.enabled", "true");
//        sqlMerge =
//                "MERGE INTO demo.merge_tgt tgt " +
//                "USING demo.merge_src src ON tgt.ID = src.ID " +
//                "WHEN MATCHED THEN UPDATE SET * " +
//                "WHEN NOT MATCHED THEN INSERT *";
//        spark.sql(sqlMerge);
//        spark.sql("SELECT * FROM demo.merge_tgt ORDER BY ID").show();
//
//
//        // DELETE
//        spark.sql("DELETE FROM demo.merge_tgt WHERE ID > 3");
//        spark.sql("SELECT * FROM demo.merge_tgt ORDER BY ID").show();
//
//        spark.sql("DROP TABLE IF EXISTS demo.merge_src");
//
//        // TIME TRAVEL
//
//        spark.sql("DESCRIBE HISTORY demo.merge_tgt").show();
//        Dataset<Row> dfHistory = spark.sql("DESCRIBE HISTORY demo.merge_tgt").toDF();
//        List<Row> h = dfHistory.collectAsList();
//
//        dfHistory.selectExpr("max(version) as maxVersion", "max(timestamp) as maxTimestamp").show();
//        spark.read().format("delta")
//                .option("versionAsOf", "4")
//                .load("C:\\TemenosWork\\2022\\Lakehouse\\DeltaLake\\spark-warehouse\\demo.db\\merge_tgt")
//                .show();
//
//        spark.read().format("delta")
//                .option("versionAsOf", "5")
//                .load("C:\\TemenosWork\\2022\\Lakehouse\\DeltaLake\\spark-warehouse\\demo.db\\merge_tgt")
//                .show();
//
//        spark.sql("DROP TABLE IF EXISTS demo.merge_tgt");
//
//
//        // Create table using DataFrameWriter v2 API
//        Dataset<Row> df = spark.sql("SELECT * FROM demo." + tableName).toDF();
//        df.write()
//            .format("delta")
//            .mode("overwrite")
//            .partitionBy("StateProvinceName", "City")
//            .saveAsTable("demo.anotherEmp");
//        spark.sql("SELECT * FROM demo.anotherEmp").show();
//
//        // Unmanaged, external or path-based
//        sqlDropDb = "DROP DATABASE IF EXISTS demo_um";
//        spark.sql(sqlDropDb);
//
//        /* Deleting directory in Windows. Use different methods if on AWS, Azure or GCP */
//        tableDir = new File("C:/TemenosWork/2022/Lakehouse/DeltaLake/UnmanagedTables/employee2");
//        if (tableDir.exists()) {
//            try {
//                FileUtils.deleteDirectory(tableDir);
//                logger.info("Directory and content deleted under: " + tableDir.toString());
//            } catch (IOException e) {
//                logger.error("Deleting directory issue: " + e.getMessage());
//                throw new RuntimeException(e);
//            }
//        }
//
//        sqlCreateDb = "CREATE DATABASE demo_um LOCATION 'C:/TemenosWork/2022/Lakehouse/DeltaLake/UnmanagedTables'";
//        spark.sql(sqlCreateDb);
//
//
//        spark.sql("USE demo_um");
//        sqlCreateTable = "CREATE TABLE IF NOT EXISTS " + tableName + "2" +
//                " (" +
//                "BusinessEntityID INT NOT NULL, Title STRING, FirstName STRING, MiddleName STRING, " +
//                "LastName STRING, Suffix STRING, JobTitle STRING, PhoneNumber STRING, " +
//                "PhoneNumberType STRING, EmailAddress STRING, EmailPromotion STRING, " +
//                "AddressLine1 STRING, AddressLine2 STRING, City STRING, StateProvinceName STRING," +
//                "PostalCode STRING, CountryRegionName STRING" +
//                " )" +
//                " USING DELTA" +
//                " PARTITIONED BY (StateProvinceName)" +
//                " LOCATION 'C:/TemenosWork/2022/Lakehouse/DeltaLake/UnmanagedTables/" + tableName + "2'";
//
//        spark.sql(sqlCreateTable);
//
//        spark.sql("SHOW TABLES").show(10);
//
//        sqlInsert = sqlInsert.replaceFirst("demo." + tableName, "demo_um." + tableName + "2");
//        spark.sql(sqlInsert);
//
//        spark.sql("SELECT * FROM demo_um." + tableName + "2").show(10);
//
//        // Using DataTableBuilder API, not SQL
//        spark.sql("DROP TABLE IF EXISTS default.employee_java");
//
//        /* Deleting directory in Windows. Use different methods if on AWS, Azure or GCP */
//        tableDir = new File("C:/TemenosWork/2022/Lakehouse/DeltaLake/spark-warehouse/employee_java");
//        if (tableDir.exists()) {
//            try {
//                FileUtils.deleteDirectory(tableDir);
//                logger.info("Directory and content deleted under: " + tableDir.toString());
//            } catch (IOException e) {
//                logger.error("Deleting directory issue: " + e.getMessage());
//                throw new RuntimeException(e);
//            }
//        }
//
//        String sqlCreate = "CREATE TABLE default.employee_java (EmployeeID INT) USING DELTA";
//        spark.sql(sqlCreate);
//
//        DeltaTable
//                .createOrReplace(spark)
//                .tableName("default.employee_java")
//                .partitionedBy("StateProvinceName")
//                .comment("table created by DataTableBuilder API")
//                .addColumn("EmployeeID", "INT", false)
//                .addColumn("StateProvinceName", "String", false)
//                .addColumn(
//                        DeltaTable.columnBuilder("EmpID_Plus_1000")
//                            .dataType("INT")
//                            .generatedAlwaysAs("EmployeeID + 1000")
//                            .comment("comments or no comments?")
//                            .build()
//                )
//                .execute();
//        spark.sql("SELECT * FROM default.employee_java").show();
//        spark.sql("INSERT INTO default.employee_java (EmployeeID, StateProvinceName) VALUES (1, 'BC'), (2, 'AB'), (3, 'QC')");
//        spark.sql("SELECT * FROM default.employee_java ORDER BY EmployeeID").show();

        spark.close();
        logger.info("completed");
    }
}