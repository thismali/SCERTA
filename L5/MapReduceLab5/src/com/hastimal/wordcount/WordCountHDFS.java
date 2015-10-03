package com.hastimal.wordcount;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.google.protobuf.ByteString.Output;
        
public class WordCountHDFS {
        
 public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);  //
        }
    }
 } 
        
 public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
//apple 1,1
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum)); //apple,2
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	Job job = new Job(conf, "WordCountHDFS");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(WordCountMap.class);
    job.setCombinerClass(WordCountReduce.class);
    job.setReducerClass(WordCountReduce.class);
    job.setNumReduceTasks(2);
       
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class); //Binary format for video audio
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input/wikidoc"));
//    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output/wikidoc"));
   // hdfs://localhost:9000/input/wikidoc hdfs://localhost:9000/output/wikidoc
   //job.waitForCompletion(true);
    //ARG:hdfs://localhost:9000/input/document hdfs://localhost:9000/output/document
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
        
}