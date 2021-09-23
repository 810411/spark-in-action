package common.exif;

import common.extlib.RecursiveExtensionFilteredLister;
import common.utils.K;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import scala.collection.immutable.Map;

import java.util.Locale;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;

public class ExifDirectoryDataSource implements RelationProvider {
    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        java.util.Map<String, String> optionsAsJavaMap = mapAsJavaMapConverter(parameters).asJava();

        ExifDirectoryRelation br = new ExifDirectoryRelation();
        br.setSqlContext(sqlContext);
        RecursiveExtensionFilteredLister photoLister = new RecursiveExtensionFilteredLister();

        for(java.util.Map.Entry<String, String> entry: optionsAsJavaMap.entrySet()) {
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue();

            switch (key) {
                case K.PATH:
                    photoLister.setPath(value);
                    break;
                case K.RECURSIVE:
                    if (value.toLowerCase().charAt(0) == 't') {
                        photoLister.setRecursive(true);
                    } else {
                        photoLister.setRecursive(false);
                    }
                    break;
            }
        }

        br.setPhotoLister(photoLister);
        return br;
    }
}
