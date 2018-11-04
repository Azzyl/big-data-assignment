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

    public void setWordId(LongWritable wordId) {
        this.wordId = wordId;
    }

    public void setInverseDocumentFrequency(LongWritable inverseDocumentFrequency) {
        this.InverseDocumentFrequency = inverseDocumentFrequency;
    }

    public IndexIDF() {
        wordId = new LongWritable(0L);
        InverseDocumentFrequency = new LongWritable(0L);
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
