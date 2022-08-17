package com.temenos.analytics.deltalake;

import io.delta.tables.DeltaTable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class Lakehouse {
    private static final Logger logger = LogManager.getLogger(Lakehouse.class);

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

        String lakehouseDir = FilenameUtils.separatorsToSystem(spark.conf().get("spark.sql.warehouse.dir"));
        lakehouseDir = lakehouseDir.replaceFirst("file:\\\\", "");

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

        spark.sql("SHOW TABLES IN demo").show(10);

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
        spark.sql("SELECT * FROM demo." + tableName).show(10);

        sqlDropDb = "DROP DATABASE IF EXISTS demo_um";
        spark.sql(sqlDropDb);

        /* Deleting directory in Windows. Use different methods if on AWS, Azure or GCP */
        tableDir = new File("C:/TemenosWork/2022/Lakehouse/DeltaLake/UnmanagedTables/employee2");
        if (tableDir.exists()) {
            try {
                FileUtils.deleteDirectory(tableDir);
                logger.info("Directory and content deleted under: " + tableDir.toString());
            } catch (IOException e) {
                logger.error("Deleting directory issue: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }

        sqlCreateDb = "CREATE DATABASE demo_um LOCATION 'C:/TemenosWork/2022/Lakehouse/DeltaLake/UnmanagedTables'";
        spark.sql(sqlCreateDb);


        spark.sql("USE demo_um");
        sqlCreateTable = "CREATE TABLE IF NOT EXISTS " + tableName + "2" +
                " (" +
                "BusinessEntityID INT NOT NULL, Title STRING, FirstName STRING, MiddleName STRING, " +
                "LastName STRING, Suffix STRING, JobTitle STRING, PhoneNumber STRING, " +
                "PhoneNumberType STRING, EmailAddress STRING, EmailPromotion STRING, " +
                "AddressLine1 STRING, AddressLine2 STRING, City STRING, StateProvinceName STRING," +
                "PostalCode STRING, CountryRegionName STRING" +
                " )" +
                " USING DELTA" +
                " PARTITIONED BY (StateProvinceName)" +
                " LOCATION 'C:/TemenosWork/2022/Lakehouse/DeltaLake/UnmanagedTables/" + tableName + "2'";

        spark.sql(sqlCreateTable);

        spark.sql("SHOW TABLES").show(10);

        sqlInsert = sqlInsert.replaceFirst("demo." + tableName, "demo_um." + tableName + "2");
        spark.sql(sqlInsert);

        spark.sql("SELECT * FROM demo_um." + tableName + "2").show(10);

        // USING JAVA API
        spark.sql("DROP TABLE IF EXISTS default.employee_java");

        /* Deleting directory in Windows. Use different methods if on AWS, Azure or GCP */
        tableDir = new File("C:/TemenosWork/2022/Lakehouse/DeltaLake/spark-warehouse/employee_java");
        if (tableDir.exists()) {
            try {
                FileUtils.deleteDirectory(tableDir);
                logger.info("Directory and content deleted under: " + tableDir.toString());
            } catch (IOException e) {
                logger.error("Deleting directory issue: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }

        String sqlCreate = "CREATE TABLE default.employee_java (EmployeeID INT) USING DELTA";
        spark.sql(sqlCreate);

        DeltaTable.createOrReplace(spark)
                .tableName("default.employee_java")
                .partitionedBy("StateProvinceName")
                .comment("table created by path; supposed being outside of the hive metastore but...")
                .addColumn("EmployeeID", "INT", false)
                .addColumn("StateProvinceName", "String", false)
                .execute();
        spark.sql("SELECT * FROM default.employee_java").show();
        spark.sql("INSERT INTO default.employee_java (EmployeeID, StateProvinceName) VALUES (1, 'BC'), (2, 'AB'), (3, 'QC')");
        spark.sql("SELECT * FROM default.employee_java ORDER BY EmployeeID").show();

        spark.close();
        logger.info("completed");
    }
}