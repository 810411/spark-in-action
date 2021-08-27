import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ComplexCsvToDataframeApp {
    public static void main(String[] args) {
        ComplexCsvToDataframeApp app = new ComplexCsvToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV to DataFrame")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel(LogLevel.WARN.toString());

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "*")
                .option("dateFormat", "M/d/y")
                .option("inferSchema", true)
                .load("ch07/data/books.csv");

        System.out.println("Excerpt of the dataframe content:");
        df.show(7, 90);
        System.out.println("Dataframe's schema:");
        df.printSchema();
    }
}
