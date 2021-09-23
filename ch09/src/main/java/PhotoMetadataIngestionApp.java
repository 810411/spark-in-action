import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PhotoMetadataIngestionApp {
    public static void main(String[] args) {
        PhotoMetadataIngestionApp app = new PhotoMetadataIngestionApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("EXIF to Dataset")
                .master("local").getOrCreate();
        spark.sparkContext().setLogLevel(LogLevel.WARN.toString());

        Dataset<Row> df = spark.read()
                .format("exif")
                .option("recursive", "true")
                .option("limit", "100000")
                .option("extensions", "jpg,jpeg")
                .load("ch09/data");
        System.out.println("I have imported " + df.count() + " photos.");
        df.printSchema();
        df.show(5);
    }
}
