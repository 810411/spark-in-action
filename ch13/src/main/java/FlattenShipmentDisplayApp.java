import static org.apache.spark.sql.functions.explode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class FlattenShipmentDisplayApp {
    public static void main(String[] args) {
        FlattenShipmentDisplayApp app = new FlattenShipmentDisplayApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Flatenning JSON doc describing shipments")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", true)
                .load("ch13/data/json/shipment.json");

        df = df
                .withColumn("supplier_name", df.col("supplier.name"))
                .withColumn("supplier_city", df.col("supplier.city"))
                .withColumn("supplier_state", df.col("supplier.state"))
                .withColumn("supplier_country", df.col("supplier.country"))
                .drop("supplier")
                .withColumn("customer_name", df.col("customer.name"))
                .withColumn("customer_city", df.col("customer.city"))
                .withColumn("customer_state", df.col("customer.state"))
                .withColumn("customer_country", df.col("customer.country"))
                .drop("customer")
                .withColumn("items", explode(df.col("books")));
        df = df
                .withColumn("qty", df.col("items.qty"))
                .withColumn("title", df.col("items.title"))
                .drop("items")
                .drop("books");

        df.show(5, false);
        df.printSchema();

        df.createOrReplaceTempView("shipment_detail");
        Dataset<Row> bookCountDf = spark.sql("SELECT COUNT(*) AS bookCount FROM shipment_detail");
        bookCountDf.show(false);
    }
}
