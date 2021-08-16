import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class CsvToRelationalDatabaseApp {
    public static void main(String[] args) {
        CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel(LogLevel.WARN.toString());

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("ch02/data/authors.csv");

        df = df.withColumn(
                "name",
                concat(
                        df.col("lname"),
                        lit(", "),
                        df.col("fname")
                ));

        String dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs";
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "postgres");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "ch02", prop);

        System.out.println("Complete");
    }
}
