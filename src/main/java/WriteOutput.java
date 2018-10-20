import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

public class WriteOutput {
    private static final Logger logger = LogManager.getLogger(WriteOutput.class);

    private BufferedWriter out = null;
    private final String filename;

    WriteOutput(final String filename) {
        this.filename = filename;
    }

    void open() throws IOException {
        logger.info("Open new output file: " + filename);

        FileWriter fstream = new FileWriter(filename, true);
        this.out = new BufferedWriter(fstream);
    }

    public void write(final String text) throws IOException {
        this.out.write(text);
    }

    public void writeln(final String text) throws IOException {
        this.out.write(text);
        this.out.newLine();
    }


    public void writeOutputFooter(final int recordCount) throws IOException {
        final Date currentDate = new Date();

        logger.info("Write footer for the output file with record count: " + recordCount);

        writeln("END_OF_FILE");
        writeln("Time Finished=" + currentDate);
        writeln("RECORD_COUNT=" + recordCount);
    }

    private void delete(final String filename) throws IOException {
        logger.info("Delete output file if it exists: " + filename);

        Files.deleteIfExists(Paths.get(filename));
    }

    public void newLine() throws IOException {
        this.out.newLine();
    }

    public void close() throws IOException {
        logger.info("Close output file.");

        out.close();
    }
}
