import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Driver {

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        String inputDir = args[0]; //input dir
        String nGramLib = args[1]; //output dir from 1st job, job2 don't need input dir anymore
        String numberOfNGram = args[2];
        String threshold = args[3];
        String topK = args[4];

        //job1
        Configuration config1 = new Configuration();
        config1.set("textinputformat.record.delimiter", "."); // set to read sentence by sentence
        config1.set("noGram", numberOfNGram);

        Job job1 = Job.getInstance(config1);
        job1.setJobName("NgramLibrary");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(inputDir));
        TextOutputFormat.setOutputPath(job1, new Path(nGramLib));

        //job2
        Configuration config2 = new Configuration();
        config2.set("threshold", threshold);
        config2.set("topK", topK);

        // connect to database, formatted. //test is the name of database;
        DBConfiguration.configureDB(config2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.1.107:3306/test",
                "root",
                "root");

        Job job2 = Job.getInstance(config2);
        job2.setJobName("ProbabilityModel");
        job2.setJarByClass(Driver.class);

        job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar")); //add dependency to hdfs when run hadoop
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setMapperClass(ProbModel.Map.class);
        job2.setReducerClass(ProbModel.Reduce.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        DBOutputFormat.setOutput(job2, "", new String[]{" inputWords", "followingWords", "value"});

        TextInputFormat.setInputPaths(job2,args[1]);
        job2.waitForCompletion(true);

    }
}
