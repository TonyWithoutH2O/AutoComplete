import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;


public class ProbModel {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        int threshold;
        public void setup(Context context) {
            Configuration config = new Configuration();
            threshold = config.getInt("threshold",  20);
        }

        // map
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input : Key: Ngram value :count
            // what does mapper do, split by position n - 1
            String line = value.toString();
            String[] words_count = line.split("\t");
            if (words_count.length < 2) {
                return;
            }

            int count = Integer.parseInt(words_count[1]);
            if (count < threshold) {
                return; // do nothing if less than threshold
            }

            StringBuilder sb = new StringBuilder();
            String[] words = words_count[0].split("\\s+");
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i] + " ");
            }

            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1] + "=" + count;
            context.write(new Text(outputKey), new Text(outputValue));

        }
    }



    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        // reduce method
        int topK;


        public void setup(Context context) {
            Configuration config = new Configuration();
            topK = config.getInt("topK", 5);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key = this is ; value = <cool=20, girl =200, boy=125>
            //topK -> write to database
            //PQ -> word+count(pair) --> comparator

            PriorityQueue<Text> pq = new PriorityQueue<Text>(new Comparator<Text>() {
                public int compare(Text a, Text b) {
                    String[] A = a.toString().trim().split("=");
                    String[] B = b.toString().trim().split("=");
                    int count_A = Integer.parseInt(A[1]);
                    int count_B = Integer.parseInt(B[1]);
                    String word_A = A[0], word_B = B[0];

                    if (count_A > count_B) {
                        return -1;
                    }
                    if (count_A < count_B) {
                        return 1;
                    }
                    return word_A.compareTo(word_B);
                }
            });

            for (Text elem : values) {
                pq.offer(elem);
            }

            for (int i = 0; i <= topK && !pq.isEmpty(); i++) {
                String[] temp = pq.poll().toString().trim().split("=");
                String DBcolumn2 = temp[0];
                int DBcolumns3 = Integer.parseInt(temp[1]);
                context.write(new DBOutputWritable(key.toString(), DBcolumn2, DBcolumns3), NullWritable.get());
            }
            return;
        }
    }
}
