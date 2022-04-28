
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.List;

public class MainReadParquet {

    public static void main(String[] args) throws IllegalArgumentException {
        try {
            MainReadParquet.readFile(args[0],
                    args[0]);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void readFile(final String inputPath, final String outputPath) throws IOException {
        final Path path = new Path("file:\\" + inputPath);
        WriteOutput writeOutput = new WriteOutput(outputPath);
        boolean isFirst = true;
        Configuration conf = new Configuration();

        writeOutput.open();
        ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        MessageType schema = readFooter.getFileMetaData().getSchema();
        ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);

        StringBuilder stringBuilder;
        PageReadStore pages;

        int totalCount = 0;
        while (null != (pages = r.readNextRowGroup())) {
            final long rows = pages.getRowCount();

            final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
            for (int i = 0; i < rows; i++) {
                if (isFirst) {
                    stringBuilder = getHeaders(schema);
                    writeOutput.writeln(stringBuilder.toString());
                    isFirst = false;
                }

                final Group g = recordReader.read();
                stringBuilder = getRow(g);
                writeOutput.writeln(stringBuilder.toString());
            }
            totalCount += rows;
            System.out.println("Wrote " + rows + " rows to file.");
        }

        writeOutput.writeOutputFooter(totalCount);
        writeOutput.close();
        r.close();
    }

    private static StringBuilder getHeaders(final MessageType schema) {
        boolean isFirst = true;
        StringBuilder returnVal = new StringBuilder(1000);
        List<Type> typeList = schema.getFields();

        for(Type column : typeList) {
            if(isFirst) {
                returnVal.append(column.getName());
                isFirst = false;
            }
            else {
                returnVal.append("|");
                returnVal.append(column.getName());
            }
        }

        return returnVal;
    }

    private static StringBuilder getRow(final Group row) {
        boolean isFirst = true;
        StringBuilder returnVal = new StringBuilder(1000);

        int columnCount = row.getType().getFieldCount();
        for(int index = 0; index < columnCount; index++) {
            if(isFirst) {
                if(row.getFieldRepetitionCount(index) == 1) {
                    returnVal.append(row.getValueToString(index, 0));
                }
                isFirst = false;
            }
            else {
                returnVal.append("|");
                if(row.getFieldRepetitionCount(index) == 1) {
                    returnVal.append(row.getValueToString(index, 0));
                }
            }
        }

        return returnVal;
    }
}
