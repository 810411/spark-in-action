import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SQLToDatasetApp {
    public static void main(String[] args) {
        SQLToDatasetApp app = new SQLToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("SQL to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel(LogLevel.WARN.toString());

        Properties properties = new Properties();
        properties.put("user", "postgres");
        properties.put("password", "postgres");

        Dataset<Row> df = spark.read()
                .jdbc(
                        "jdbc:postgresql://localhost/sakila?serverTimezone=EST",
                        "actor",
                        properties
                );

        df = df.orderBy(df.col("last_name"));
        df.show(5);
        df.printSchema();
        System.out.println("Dataframe records count = " + df.count());
        System.out.println("The dataframe is split over " + df.rdd()
                .getPartitions().length + " partition(s).");
    }
}
