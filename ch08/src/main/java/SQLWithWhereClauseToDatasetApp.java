import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SQLWithWhereClauseToDatasetApp {
    public static void main(String[] args) {
        SQLWithWhereClauseToDatasetApp app =
                new SQLWithWhereClauseToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("SQL with where clause to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel(LogLevel.WARN.toString());

        Properties properties = new Properties();
        properties.put("user", "postgres");
        properties.put("password", "postgres");

        String sqlQuery = "select * from film where "
                + "(title ilike '%ALIEN%' or title ilike '%victory%' "
                + "or title ilike '%agent%' or description ilike '%action%') "
                + "and rental_rate>1 "
                + "and (rating='G' or rating='PG')";

        Dataset<Row> df = spark.read()
                .jdbc(
                        "jdbc:postgresql://localhost/sakila?serverTimezone=EST",
                        "(" + sqlQuery + ") film_alias",
                        properties
                );

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");
    }

}
