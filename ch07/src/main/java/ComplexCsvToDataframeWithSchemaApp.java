import org.apache.log4j.lf5.LogLevel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ComplexCsvToDataframeWithSchemaApp {
    public static void main(String[] args) {
        ComplexCsvToDataframeWithSchemaApp app = new ComplexCsvToDataframeWithSchemaApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV with a schema to Dataframe")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel(LogLevel.WARN.toString());

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("authordId", DataTypes.IntegerType, true),
                DataTypes.createStructField("bookTitle", DataTypes.StringType, false),
                DataTypes.createStructField("releaseDate", DataTypes.DateType,true),
                DataTypes.createStructField("url", DataTypes.StringType,false)
        });

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("dateFormat", "MM/dd/yyyy")
                .option("quote", "*")
                .schema(schema)
                .load("ch07/data/books.csv");

        df.show(5, 15);
        df.printSchema();
    }
}
