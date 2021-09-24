package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.lib.*;

import java.io.File;
import java.nio.file.Paths;

public class RecordsInFilesGeneratorApp {
    private static Logger log = LoggerFactory.getLogger(RecordsInFilesGeneratorApp.class);

    public int streamDuration = 60;

    public int batchSize = 10;

    public int waitTime = 5;

    public static void main(String[] args) {
        String outputDirectory = StreamingUtils.getInputDirectory();
        if (args.length == 2
                && args[0].compareToIgnoreCase("--output-directory") == 0) {
            outputDirectory = args[1];
            File dir = new File(outputDirectory);
            dir.mkdir();
        }

        RecordStructure rs = new RecordStructure("contact")
                .add("fname", FieldType.FIRST_NAME)
                .add("mname", FieldType.FIRST_NAME)
                .add("lname", FieldType.LAST_NAME)
                .add("age", FieldType.AGE)
                .add("ssn", FieldType.SSN);

        RecordsInFilesGeneratorApp app = new RecordsInFilesGeneratorApp();
        app.start(rs, outputDirectory);
    }

    private void start(RecordStructure rs, String outputDirectory) {
        log.debug("-> start (..., {})", outputDirectory);
        long start = System.currentTimeMillis();
        while (start + streamDuration * 1000 > System.currentTimeMillis()) {
            int maxRecord = RecordGeneratorUtils.getRandomInt(batchSize) + 1;
            RecordWriterUtils.write(
                    rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt",
                    rs.getRecords(maxRecord, false),
                    outputDirectory);
            try {
                Thread.sleep(RecordGeneratorUtils.getRandomInt(waitTime * 1000)
                        + waitTime * 1000 / 2);
            } catch (InterruptedException e) {
                // Simply ignore the interruption
            }
        }
    }
}
