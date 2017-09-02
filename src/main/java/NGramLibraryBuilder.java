import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibraryBuilder {

    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int noGram;

        @Override
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            noGram = config.getInt("noGram", 5);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //read a single sentence
            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]]", " ");
            String[] words = line.split("\\s+");
            if (words.length < 2) {
                return;
            }

            // (n - 1) split
            for (int i = 0; i < words.length; i++) {
                StringBuilder temp = new StringBuilder();
                for (int j = 1; i + j < words.length && j < noGram; j++) {
                    temp.append(" ");
                    temp.append(words[i + j]);
                    context.write(new Text(temp.toString().trim()), new IntWritable(1));   // write to disk

                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text,IntWritable> {
        // reduce method

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

}