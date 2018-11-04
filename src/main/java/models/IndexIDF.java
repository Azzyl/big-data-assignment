package models;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IndexIDF implements Writable {
    private LongWritable wordId;
    private LongWritable InverseDocumentFrequency;

    public LongWritable getInverseDocumentFrequency() {
        return InverseDocumentFrequency;
    }

    public LongWritable getWordId() {
        return wordId;
    }

    public IndexIDF(LongWritable wordId, LongWritable InverseDocumentFrequency) {
        this.wordId = wordId;
        this.InverseDocumentFrequency = InverseDocumentFrequency;
    }

    public void write(DataOutput dataOutput) throws IOException {
        wordId.write(dataOutput);
        InverseDocumentFrequency.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        wordId.readFields(dataInput);
        InverseDocumentFrequency.readFields(dataInput);
    }
}
