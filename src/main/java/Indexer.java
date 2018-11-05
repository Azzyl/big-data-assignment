import models.Document;
import models.IndexIDF;
import models.IndexNormalized;
import models.VocabularyItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

public class Indexer {

    public static class VocabularyMapper
            extends Mapper<Object, Text, Text, LongWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) {
            Document document = new Document();
            document.parseJSON(value.toString());
            String text = document.getText().toLowerCase().replaceAll("[?!,.'()*]", "");
            StringTokenizer itr = new StringTokenizer(text);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                try {
                    context.write(word, new LongWritable(document.getId()));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class VocabularyReducer
            extends Reducer<Text, LongWritable, Text, NullWritable> {

        static Long counter = 0L;

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            HashSet<LongWritable> amounts = new HashSet<>();
            for (LongWritable v : values) {
                amounts.add(v);
            }

            VocabularyItem entry = new VocabularyItem(key.toString(), counter, amounts.size());
            counter++;
            context.write(new Text(entry.createJSON()), NullWritable.get());
        }
    }

    public static class IndexerMapper
            extends Mapper<Object, Text, LongWritable, IndexIDF> {

        private HashMap<String, VocabularyItem> words;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path vocabularyPath = new Path(conf.get("vocabulary"));
            FileStatus[] fileStatuses = fs.listStatus(vocabularyPath);
            words = new HashMap<>();
            for (FileStatus s : fileStatuses) {
                createVocabulary(fs, s.getPath());
            }
        }

        private void createVocabulary(FileSystem fs, Path pathToFile) throws IOException {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pathToFile)));
            String line = reader.readLine();
            try {
                while (line != null) {
                    VocabularyItem vocabularyItem = VocabularyItem.parseJSON(line);
                    words.put(vocabularyItem.getWord(), vocabularyItem);
                    line = reader.readLine();
                }
            } finally {
                reader.close();
            }

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Document document = new Document();
            document.parseJSON(value.toString());
            String text = document.getText().toLowerCase().replaceAll("[?!,.'()*]", "");
            StringTokenizer itr = new StringTokenizer(text);
            while (itr.hasMoreTokens()) {
                VocabularyItem item = words.get(itr.nextToken());
                IndexIDF indexIDF = new IndexIDF(new LongWritable(item.getWordId()), new LongWritable(item.getInverseDocumentFrequency()));

                context.write(new LongWritable(document.getId()), indexIDF);
            }
        }
    }

    public static class IndexerReducer extends Reducer<LongWritable, IndexIDF, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<IndexIDF> values, Context context) throws IOException, InterruptedException {
            HashMap<Long, Long> counts = new HashMap<>();
            HashMap<Long, Long> idfs = new HashMap<>();
            ArrayList<IndexNormalized> indexes = new ArrayList<>();

            for (IndexIDF indexIDF : values) {
                Long wordId = indexIDF.getWordId().get();
                counts.putIfAbsent(wordId, 0L);
                Long before = counts.get(wordId);
                counts.remove(wordId);
                counts.put(wordId, before + 1);
                idfs.put(wordId, indexIDF.getInverseDocumentFrequency().get());
            }
            for (Long k: counts.keySet()) {
                indexes.add(new IndexNormalized(k, (double) counts.get(k)/ idfs.get(k)));
            }
            JSONArray array = new JSONArray();
            for (IndexNormalized index: indexes) {
                array.put(index.createJSON());
            }
            context.write(key, new Text(array.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration confVocabulary = new Configuration();
        Job jobVocabulary = Job.getInstance(confVocabulary, "vocabulary");
        jobVocabulary.setJarByClass(Indexer.class);
        jobVocabulary.setMapperClass(VocabularyMapper.class);
        jobVocabulary.setReducerClass(VocabularyReducer.class);
        jobVocabulary.setMapOutputKeyClass(Text.class);
        jobVocabulary.setMapOutputValueClass(LongWritable.class);
        jobVocabulary.setOutputKeyClass(Text.class);
        jobVocabulary.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(jobVocabulary, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobVocabulary, new Path("vocabulary"));

        Configuration confIndexer = new Configuration();
        confIndexer.set("vocabulary", "vocabulary");
        Job jobIndexer = Job.getInstance(confIndexer, "indexes");
        jobIndexer.setJarByClass(Indexer.class);
        jobIndexer.setMapperClass(IndexerMapper.class);
        jobIndexer.setMapOutputKeyClass(LongWritable.class);
        jobIndexer.setMapOutputValueClass(IndexIDF.class);
        jobIndexer.setReducerClass(IndexerReducer.class);
        jobIndexer.setOutputKeyClass(LongWritable.class);
        jobIndexer.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobIndexer, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobIndexer, new Path("indexes"));

        jobVocabulary.waitForCompletion(true);
        System.exit(jobIndexer.waitForCompletion(true) ? 0 : 1);
    }
}
