package models;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IndexIDF implements Writable {
    private LongWritable wordId;
    private LongWritable inverseDocumentFrequency;

    public LongWritable getInverseDocumentFrequency() {
        return inverseDocumentFrequency;
    }

    public LongWritable getWordId() {
        return wordId;
    }

    public void setWordId(LongWritable wordId) {
        this.wordId = wordId;
    }

    public void setInverseDocumentFrequency(LongWritable inverseDocumentFrequency) {
        this.inverseDocumentFrequency = inverseDocumentFrequency;
    }

    public IndexIDF() {
        wordId = new LongWritable(0L);
        inverseDocumentFrequency = new LongWritable(0L);
    }

    public IndexIDF(LongWritable wordId, LongWritable inverseDocumentFrequency) {
        this.wordId = wordId;
        this.inverseDocumentFrequency = inverseDocumentFrequency;
    }

    public void write(DataOutput dataOutput) throws IOException {
        wordId.write(dataOutput);
        inverseDocumentFrequency.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        wordId.readFields(dataInput);
        inverseDocumentFrequency.readFields(dataInput);
    }
}
