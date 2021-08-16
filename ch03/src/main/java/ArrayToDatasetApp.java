import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class ArrayToDatasetApp {
    public static void main(String[] args) {
        ArrayToDatasetApp app = new ArrayToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Array to Dataset<String>")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel(LogLevel.WARN.toString());

        List<String> data = List.of("Apple", "Kiwi", "Peach", "Banana");

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        ds.printSchema();
        ds.show();

        System.out.println("=".repeat(32));
        Dataset<Row> df = ds.toDF();
        df.printSchema();
        df.show();
    }
}
