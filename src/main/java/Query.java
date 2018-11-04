import models.IndexNormalized;
import models.VocabularyItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

public class Query {

    public static class QueryMapper extends Mapper<Text, Text, DoubleWritable, LongWritable> {

        private HashMap<String, VocabularyItem> words;
        private List<IndexNormalized> queryVector;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path vocabularyPath = new Path(conf.get("vocabulary"));
            FileStatus[] fileStatuses = fs.listStatus(vocabularyPath);
            words = new HashMap<>();
            HashMap<Long, Long> counts = new HashMap<>();
            HashMap<Long, Long> idfs = new HashMap<>();
            queryVector = new ArrayList<>();
            for (FileStatus s: fileStatuses) {
                createVocabulary(fs, s.getPath());
            }
            String query = conf.get("query").toLowerCase();
            StringTokenizer itr = new StringTokenizer(query);
            while (itr.hasMoreElements()) {
                String word = itr.nextToken();
                VocabularyItem item = words.get(word);
                Long wordId = item.getWordId();
                counts.putIfAbsent(wordId, 0L);
                Long before = counts.get(wordId);
                counts.remove(wordId);
                counts.put(wordId, before + 1);
                idfs.putIfAbsent(wordId, words.get(word).getInverseDocumentFrequency());
            }
            for (Long k: counts.keySet()) {
                IndexNormalized indexIDF = new IndexNormalized(k,(double) counts.get(k) / idfs.get(k));
                queryVector.add(indexIDF);
            }
        }

        private void createVocabulary(FileSystem fs, Path pathToFile) throws IOException {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pathToFile)));
            try {
                String line = reader.readLine();
                while (line != null) {
                    VocabularyItem item = VocabularyItem.parseJSON(line);
                    words.put(item.getWord(), item);
                    line = reader.readLine();
                }
            } finally {
                reader.close();
            }
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<IndexNormalized> indexes = IndexNormalized.parseJSONArray(new JSONArray(value.toString()));
            double sum = 0;
            for (int i = 0; i < indexes.size(); i++) {
                IndexNormalized index = indexes.get(i);
                for (int j = 0; j < queryVector.size(); j++) {
                    IndexNormalized queryIndex = queryVector.get(j);
                    if (index.getWordId() == queryIndex.getWordId()) {
                        sum += index.getTF_IDF() * queryIndex.getTF_IDF();
                    }
                }
            }
            context.write(new DoubleWritable((-1) * sum), new LongWritable(Long.parseLong(key.toString())));
        }
    }

    public static class QueryReducer extends Reducer<DoubleWritable, LongWritable, Text, Text> {

        enum CountersEnum { LIMIT_OUTPUT }

        @Override
        public void reduce(DoubleWritable key, Iterable<LongWritable> values, Context context) {
            Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.LIMIT_OUTPUT.toString());
            String keyTemplate = "doc ";
            String valueTemplate = ": ";
            try {
                if (counter.getValue() < 10)
                for (LongWritable val : values)
                    context.write(new Text(keyTemplate + val), new Text(valueTemplate + String.valueOf((-1) * key.get())));
                counter.increment(1);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration confQuery = new Configuration();
        confQuery.set("vocabulary", "vocabulary");
        String query = "";
        for (int i = 0; i < args.length; i++) {
            query += args[i] + " ";
        }
        confQuery.set("query", query);
        confQuery.set("key.value.separator.in.input.line", "\t");

        Job jobQuery = Job.getInstance(confQuery, "query");
        jobQuery.setInputFormatClass(KeyValueTextInputFormat.class);
        jobQuery.setJarByClass(Query.class);
        jobQuery.setMapperClass(QueryMapper.class);
        jobQuery.setMapOutputKeyClass(DoubleWritable.class);
        jobQuery.setMapOutputValueClass(LongWritable.class);
        jobQuery.setReducerClass(QueryReducer.class);
        jobQuery.setOutputKeyClass(Text.class);
        jobQuery.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobQuery, new Path("indexes"));
        FileOutputFormat.setOutputPath(jobQuery, new Path("results"));

        System.exit(jobQuery.waitForCompletion(true) ? 0 : 1);
    }
}
