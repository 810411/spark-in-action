import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PointsPerOrderApp {
    private static Logger log = LoggerFactory.getLogger(PointsPerOrderApp.class);

    public static void main(String[] args) {
        PointsPerOrderApp app = new PointsPerOrderApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Orders loyalty point")
                .master("local[*]")
                .getOrCreate();

        spark.udf().register("pointAttribution", new PointAttributionUdaf());

        // Reads a CSV file with header, called orders.csv, stores it in a
        // dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("ch15/data/orders/orders.csv");

        // Calculating the points for each customer, not each order
        Dataset<Row> pointDf = df
                .groupBy(col("firstName"), col("lastName"), col("state"))
                .agg(
                        sum("quantity"),
                        callUDF("pointAttribution", col("quantity")).as("point"));
        pointDf.show(20);

        // Alternate way: calculate order by order
        int max = PointAttributionUdaf.MAX_POINT_PER_ORDER;
        Dataset<Row> eachOrderDf = df
                .withColumn(
                        "point",
                        when(col("quantity").$greater(max), max)
                                .otherwise(col("quantity")))
                .groupBy(col("firstName"), col("lastName"), col("state"))
                .agg(
                        sum("quantity"),
                        sum("point").as("point"));
        eachOrderDf.show(20);
    }
}
