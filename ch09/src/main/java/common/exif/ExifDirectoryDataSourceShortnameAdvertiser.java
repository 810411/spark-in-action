package common.exif;

import org.apache.spark.sql.sources.DataSourceRegister;

public class ExifDirectoryDataSourceShortnameAdvertiser extends ExifDirectoryDataSource implements DataSourceRegister {
    @Override
    public String shortName() {
        return "exif";
    }
}
