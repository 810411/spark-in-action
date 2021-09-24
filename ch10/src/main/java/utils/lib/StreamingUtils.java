package utils.lib;

import java.io.File;


public class StreamingUtils {
    private String inputDirectory;
    private String inputSubDirectory1;
    private String inputSubDirectory2;
    private static StreamingUtils instance = null;

    private static StreamingUtils getInstance() {
        if (instance == null) {
            instance = new StreamingUtils();
        }
        return instance;
    }

    public static String getInputSubDirectory1() {
        return getInstance().inputSubDirectory1;
    }

    public static String getInputSubDirectory2() {
        return getInstance().inputSubDirectory2;
    }

    private StreamingUtils() {
        this.inputDirectory = "ch10/temp/";
        this.inputDirectory +=
                "streaming" + File.separator + "in" + File.separator;
        createInputDirectory(this.inputDirectory);
        this.inputSubDirectory1 += this.inputDirectory + File.separator + "s1"
                + File.separator;
        createInputDirectory(this.inputSubDirectory1);
        this.inputSubDirectory2 += this.inputDirectory + File.separator + "s2"
                + File.separator;
        createInputDirectory(this.inputSubDirectory2);
    }

    public static boolean createInputDirectory() {
        return createInputDirectory(getInputDirectory());
    }

    private static boolean createInputDirectory(String directory) {
        File d = new File(directory);
        return d.mkdirs();
    }

    public static String getInputDirectory() {
        return getInstance().inputDirectory;
    }
}
