package common.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface SparkColumn {
    String name() default "";
    String type() default "";
    boolean nullable() default true;
}