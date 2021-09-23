package common.exif;

import common.extlib.ExifUtils;
import common.extlib.PhotoMetadata;
import common.extlib.RecursiveExtensionFilteredLister;
import common.utils.Schema;
import common.utils.SparkBeanUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ExifDirectoryRelation extends BaseRelation implements Serializable, TableScan {
    private static final long serialVersionUID = 4598175080399877334L;
    private static transient Logger log = LoggerFactory.getLogger(ExifDirectoryRelation.class);
    private SQLContext sqlContext;
    private Schema schema = null;
    private RecursiveExtensionFilteredLister photoLister;


    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    public void setSqlContext(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    @Override
    public StructType schema() {
        if (schema == null) {
            schema = SparkBeanUtils.getSchemaFromBean(PhotoMetadata.class);
        }
        return schema.getSparkSchema();
    }

    @Override
    public RDD<Row> buildScan() {
        log.debug("-> buildScan()");
        schema();
        List<PhotoMetadata> table = collectData();
        JavaSparkContext sparkContext =
                new JavaSparkContext(sqlContext.sparkContext());
        JavaRDD<Row> rowRDD = sparkContext.parallelize(table)
                .map(photo -> SparkBeanUtils.getRowFromBean(schema, photo));
        return rowRDD.rdd();
    }

    private List<PhotoMetadata> collectData() {
        List<File> photosToProcess = this.photoLister.getFiles();
        List<PhotoMetadata> list = new ArrayList<>();
        PhotoMetadata photo;
        for (File photoToProcess : photosToProcess) {
            photo = ExifUtils.processFromFilename(
                    photoToProcess.getAbsolutePath());
            list.add(photo);
        }
        return list;
    }

    public void setPhotoLister(
            RecursiveExtensionFilteredLister photoLister) {
        this.photoLister = photoLister;
    }
}
