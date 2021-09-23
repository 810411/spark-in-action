package common.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkBeanUtils {
    private static Logger log = LoggerFactory.getLogger(SparkBeanUtils.class);
    private static int columnIndex = -1;

    public static Schema getSchemaFromBean(Class<?> beanClass) {
        Schema schema = new Schema();
        List<StructField> sfl = new ArrayList<>();

        Method[] methods = beanClass.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            if (!isGetter(method)) {
                continue;
            }

            String methodName = method.getName();
            SchemaColumn col = new SchemaColumn();
            col.setMethodName(methodName);

            String columnName;
            DataType dataType;
            boolean nullable;

            SparkColumn sparkColumn = method.getAnnotation(SparkColumn.class);
            if (sparkColumn == null) {
                log.debug("No annotation for method {}", methodName);
                columnName = "";
                dataType = getDataTypeFromReturnType(method);
                nullable = true;
            } else {
                columnName = sparkColumn.name();
                log.debug("Annotation for method {}, column name is {}",
                        methodName,
                        columnName);

                switch (sparkColumn.type().toLowerCase()) {
                    case "stringtype":
                    case "string":
                        dataType = DataTypes.StringType;
                        break;
                    case "binarytype":
                    case "binary":
                        dataType = DataTypes.BinaryType;
                        break;
                    case "booleantype":
                    case "boolean":
                        dataType = DataTypes.BooleanType;
                        break;
                    case "datetype":
                    case "date":
                        dataType = DataTypes.DateType;
                        break;
                    case "timestamptype":
                    case "timestamp":
                        dataType = DataTypes.TimestampType;
                        break;
                    case "calendarintervaltype":
                    case "calendarinterval":
                        dataType = DataTypes.CalendarIntervalType;
                        break;
                    case "doubletype":
                    case "double":
                        dataType = DataTypes.DoubleType;
                        break;
                    case "floattype":
                    case "float":
                        dataType = DataTypes.FloatType;
                        break;
                    case "bytetype":
                    case "byte":
                        dataType = DataTypes.ByteType;
                        break;
                    case "integertype":
                    case "integer":
                    case "int":
                        dataType = DataTypes.IntegerType;
                        break;
                    case "longtype":
                    case "long":
                        dataType = DataTypes.LongType;
                        break;
                    case "shorttype":
                    case "short":
                        dataType = DataTypes.ShortType;
                        break;
                    case "nulltype":
                    case "null":
                        dataType = DataTypes.NullType;
                        break;
                    default:
                        log.debug(
                                "Will infer data type from return type for column {}",
                                columnName);
                        dataType = getDataTypeFromReturnType(method);
                }

                nullable = sparkColumn.nullable();
            }

            String finalColumnName = buildColumnName(columnName, methodName);
            sfl.add(DataTypes.createStructField(
                    finalColumnName, dataType, nullable));
            col.setColumnName(finalColumnName);

            schema.add(col);
        }

        StructType sparkSchema = DataTypes.createStructType(sfl);
        schema.setSparkSchema(sparkSchema);
        return schema;
    }

    private static DataType getDataTypeFromReturnType(Method method) {
        String typeName = method.getReturnType().getSimpleName().toLowerCase();
        switch (typeName) {
            case "int":
            case "integer":
                return DataTypes.IntegerType;
            case "long":
                return DataTypes.LongType;
            case "float":
                return DataTypes.FloatType;
            case "boolean":
                return DataTypes.BooleanType;
            case "double":
                return DataTypes.DoubleType;
            case "string":
                return DataTypes.StringType;
            case "date":
                return DataTypes.DateType;
            case "timestamp":
                return DataTypes.TimestampType;
            case "short":
                return DataTypes.ShortType;
            case "object":
                return DataTypes.BinaryType;
            default:
                log.debug("Using default for type [{}]", typeName);
                return DataTypes.BinaryType;
        }
    }


    private static String buildColumnName(String columnName, String methodName) {
        if (columnName.length() > 0) {
            return columnName;
        }
        if (methodName.length() < 4) {
            columnIndex++;
            return "_c" + columnIndex;
        }
        columnName = methodName.substring(3);
        if (columnName.length() == 0) {
            columnIndex++;
            return "_c" + columnIndex;
        }
        return columnName;
    }

    public static Row getRowFromBean(Schema schema, Object bean) {
        List<Object> cells = new ArrayList<>();

        String[] fieldName = schema.getSparkSchema().fieldNames();
        for (int i = 0; i < fieldName.length; i++) {
            String methodName = schema.getMethodName(fieldName[i]);
            Method method;
            try {
                method = bean.getClass().getMethod(methodName);
            } catch (NoSuchMethodException e) {
                log.error("The method {} does not exists: {}.", methodName,
                        e.getMessage());
                return null;
            } catch (SecurityException e) {
                log.error("The method {} raised a security concern: {}.",
                        methodName, e.getMessage());
                return null;
            }
            try {
                cells.add(method.invoke(bean));
            } catch (IllegalAccessException e) {
                log.error("The method {} is not accessible: {}.", methodName,
                        e.getMessage());
                return null;
            } catch (IllegalArgumentException e) {
                log.error("The method {} is expecting arguments: {}.",
                        methodName, e.getMessage());
                return null;
            } catch (InvocationTargetException e) {
                log.error("The method {} raised an invocation target issue: {}.",
                        methodName, e.getMessage());
                return null;
            }
        }

        return RowFactory.create(cells.toArray());
    }

    private static boolean isGetter(Method method) {
        if (!method.getName().startsWith("get")) {
            return false;
        }
        if (method.getParameterTypes().length != 0) {
            return false;
        }
        if (void.class.equals(method.getReturnType())) {
            return false;
        }
        return true;
    }
}
