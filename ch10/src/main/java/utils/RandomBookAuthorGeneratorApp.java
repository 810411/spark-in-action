package utils;

import utils.lib.FieldType;
import utils.lib.RecordGeneratorUtils;
import utils.lib.RecordStructure;
import utils.lib.RecordWriterUtils;

public class RandomBookAuthorGeneratorApp {

    public static void main(String[] args) {
        RecordStructure rsAuthor = new RecordStructure("author")
                .add("id", FieldType.ID)
                .add("fname", FieldType.FIRST_NAME)
                .add("lname", FieldType.LAST_NAME)
                .add("dob", FieldType.DATE_LIVING_PERSON, "MM/dd/yyyy");

        RecordStructure rsBook = new RecordStructure("book", rsAuthor)
                .add("id", FieldType.ID)
                .add("title", FieldType.TITLE)
                .add("authorId", FieldType.LINKED_ID);

        RandomBookAuthorGeneratorApp app = new RandomBookAuthorGeneratorApp();
        app.start(rsAuthor, RecordGeneratorUtils.getRandomInt(4) + 2);
        app.start(rsBook, RecordGeneratorUtils.getRandomInt(10) + 1);
    }

    private void start(RecordStructure rs, int maxRecord) {
        RecordWriterUtils.write(
                rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt",
                rs.getRecords(maxRecord, true));
    }
}
