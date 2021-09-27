import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class HigherEdInstitutionPerCountyApp {
    public static void main(String[] args) {
        HigherEdInstitutionPerCountyApp app =
                new HigherEdInstitutionPerCountyApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Join")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> censusDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "cp1252")
                .load("ch12/data/census/PEP_2017_PEPANNRES.csv");
        censusDf = censusDf
                .drop("GEO.id")
                .drop("rescen42010")
                .drop("resbase42010")
                .drop("respop72010")
                .drop("respop72011")
                .drop("respop72012")
                .drop("respop72013")
                .drop("respop72014")
                .drop("respop72015")
                .drop("respop72016")
                .withColumnRenamed("respop72017", "pop2017")
                .withColumnRenamed("GEO.id2", "countyId")
                .withColumnRenamed("GEO.display-label", "county");
        System.out.println("Census data");
        censusDf.sample(0.1).show(3, false);
        censusDf.printSchema();

        Dataset<Row> higherEdDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("ch12/data/dapip/InstitutionCampus.csv");
        higherEdDf = higherEdDf
                .filter("LocationType = 'Institution'")
                .withColumn(
                        "addressElements",
                        split(higherEdDf.col("Address"), " "));
        higherEdDf = higherEdDf
                .withColumn(
                        "addressElementCount",
                        size(higherEdDf.col("addressElements")));
        higherEdDf = higherEdDf
                .withColumn(
                        "zip9",
                        element_at(
                                higherEdDf.col("addressElements"),
                                higherEdDf.col("addressElementCount")));
        higherEdDf = higherEdDf
                .withColumn(
                        "splitZipCode",
                        split(higherEdDf.col("zip9"), "-"));
        higherEdDf = higherEdDf
                .withColumn("zip", higherEdDf.col("splitZipCode").getItem(0))
                .withColumnRenamed("LocationName", "location")
                .drop("DapipId")
                .drop("OpeId")
                .drop("ParentName")
                .drop("ParentDapipId")
                .drop("LocationType")
                .drop("Address")
                .drop("GeneralPhone")
                .drop("AdminName")
                .drop("AdminPhone")
                .drop("AdminEmail")
                .drop("Fax")
                .drop("UpdateDate")
                .drop("zip9")
                .drop("addressElements")
                .drop("addressElementCount")
                .drop("splitZipCode");
        System.out.println("Higher education institutions (DAPIP)");
        higherEdDf.sample(0.1).show(3, false);
        higherEdDf.printSchema();

        Dataset<Row> countyZipDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("ch12/data/hud/COUNTY_ZIP_092018.csv");
        countyZipDf = countyZipDf
                .drop("res_ratio")
                .drop("bus_ratio")
                .drop("oth_ratio")
                .drop("tot_ratio");
        System.out.println("Counties / ZIP Codes (HUD)");
        countyZipDf.sample(0.1).show(3, false);
        countyZipDf.printSchema();

        Dataset<Row> institPerCountyDf = higherEdDf.join(
                countyZipDf,
                higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
                "inner");
        System.out.println(
                "Higher education institutions left-joined with HUD");
        institPerCountyDf
                .filter(higherEdDf.col("zip").equalTo(27517))
                .show(20, false);
        institPerCountyDf.printSchema();


        System.out.println("Attempt to drop the zip column");
        institPerCountyDf.drop("zip").sample(0.1).show(3, false);

        System.out.println("Attempt to drop the zip column");
        institPerCountyDf
                .drop(higherEdDf.col("zip"))
                .sample(0.1).show(3, false);

        institPerCountyDf = institPerCountyDf.join(
                censusDf,
                institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
                "left");

        institPerCountyDf = institPerCountyDf
                .drop(higherEdDf.col("zip"))
                .drop(countyZipDf.col("county"))
                .drop("countyId")
                .distinct();

        System.out.println("Higher education institutions in ZIP Code 27517 (NC)");
        institPerCountyDf
                .filter(higherEdDf.col("zip").equalTo(27517))
                .show(20, false);

        System.out.println("Higher education institutions in ZIP Code 02138 (MA)");
        institPerCountyDf
                .filter(higherEdDf.col("zip").equalTo(2138))
                .show(20, false);

        System.out.println("Institutions with improper counties");
        institPerCountyDf.filter("county is null").show(200, false);

        System.out.println("Final list");
        institPerCountyDf.show(200, false);
        System.out.println("The combined list has " + institPerCountyDf.count() + " elements.");

        Dataset<Row> aggDf = institPerCountyDf
                .groupBy("county", "pop2017")
                .count();
        aggDf = aggDf.orderBy(aggDf.col("count").desc());
        aggDf.show(25, false);

        Dataset<Row> popDf = aggDf
                .filter("pop2017>30000")
                .withColumn("institutionPer10k", expr("count*10000/pop2017"));
        popDf = popDf.orderBy(popDf.col("institutionPer10k").desc());
        popDf.show(25, false);
    }
}
