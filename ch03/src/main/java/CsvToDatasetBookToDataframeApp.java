import common.Book;
import common.BookMapper;
import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import java.io.Serializable;


public class CsvToDatasetBookToDataframeApp implements Serializable {
    public static void main(String[] args) {
        CsvToDatasetBookToDataframeApp app = new CsvToDatasetBookToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to dataframe to Dataset<Book> and back")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel(LogLevel.WARN.toString());

        String filename = "ch03/data/books.csv";
        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filename);

        System.out.println("*** Books ingested in a dataframe");
        df.printSchema();
        df.show();

        Dataset<Book> bookDs = df.map(
                new BookMapper(),
                Encoders.bean(Book.class)
        );

        System.out.println("*** Books are now in a dataset of books");
        bookDs.printSchema();
        bookDs.show(5, 17);

        Dataset<Row> df2 = bookDs.toDF();

        df2 = df2.withColumn(
                "releaseDateAsString",
                concat(
                        expr("releaseDate.year + 1900"), lit("-"),
                        expr("releaseDate.month + 1"), lit("-"),
                        df2.col("releaseDate.date")
                )
        );

        spark.conf().set("spark.sql.legacy.timeParserPolicy","LEGACY");
        df2 = df2.withColumn(
                "releaseDateAsDate",
                to_date(df2.col("releaseDateAsString"), "yyyy-MM-dd")
        );

        System.out.println("*** Books are now in a dataframe from dataset");
        df2.printSchema();
        df2.show(5);

    }
}


