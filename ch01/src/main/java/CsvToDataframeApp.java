import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToDataframeApp {
    public static void main(String[] args) {
        CsvToDataframeApp app = new CsvToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Dataset")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel(LogLevel.WARN.toString());

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("ch01/data/books.csv");

        df.show(10);
    }
}
