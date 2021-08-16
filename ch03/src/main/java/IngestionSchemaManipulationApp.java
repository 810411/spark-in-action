import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;


public class IngestionSchemaManipulationApp {
    public static void main(String[] args) {
        IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in Wake County, NC")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel(LogLevel.WARN.toString());

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("ch03/data/Restaurants_in_Wake_County_NC.csv");

        System.out.println("*** Schema");
        df.printSchema();

        System.out.println("We have " + df.count() + " records.");

        System.out.println("*** Right after ingestion");
        df.show(5);

        System.out.println("=".repeat(32));

        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");

        df = df.withColumn("id", concat(
                df.col("state"), lit("_"),
                df.col("county"), lit("_"),
                df.col("datasetId")));

        System.out.println("*** Dataframe transformed");
        df.printSchema();
        df.show(5);

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " + partitionCount);

        df = df.repartition(4);
        System.out.println("Partition count after repartition: " + df.rdd().partitions().length);
    }
}
