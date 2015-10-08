/****************************************************************************
 * Group #1 SCE, UMKC, 5543
 * Mohamoud, Hastimal, Jordan
 * Parralel TF*IDF using MapReduce.
 * Notes: There are three stages that each consist of a mapper and reducer. Each subsequent mapper uses the output
 * of the prior reducer as an input.
 * Final result is word and document along with its TFIDF computation.
 * 
 * INSTRUCTION: This program expects four parameters
 * arg1 = input directory (corpus directory)
 * arg2 = output directory for the first job (TF)
 * arg3 = output directory for the second job (DF)
 * arg4 = output directory for the third job (TF*IDF)
 ****************************************************************************/

package tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TFIDF {
	
	public static String corpusPath;

	public static void main(String[] args) throws IllegalArgumentException,
			ClassNotFoundException, IOException, InterruptedException {
		
		corpusPath = args[0];

		Configuration conf = new Configuration();

		Job job = new Job(conf, "TF");

		job.setJarByClass(TFIDF.class);

		runJob1(job, args[0], args[1]);

		job = new Job(conf, "IDF");

		job.setJarByClass(TFIDF.class);

		runJob2(job, args[1], args[2]);

		job = new Job(conf, "IDF");

		job.setJarByClass(TFIDF.class);

		runJob3(job, args[2], args[3]);

	}

	private static void runJob1(Job job, String inputPath, String outputPath)
			throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {

		System.out.println("Starting job 2...");
		System.out.println("Input: " + inputPath);
		System.out.println("output: " + outputPath);

		Path inputputDir = new Path(inputPath);
		Path outputDir = new Path(outputPath);

		FileSystem fs = FileSystem.get(new Configuration());

		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MapTF.class);
		job.setReducerClass(ReduceTF.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, inputputDir);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.waitForCompletion(true);
	}

	private static void runJob2(Job job, String inputPath, String outputPath)
			throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {

		System.out.println("Starting job 2...");
		System.out.println("Input: " + inputPath);
		System.out.println("output: " + outputPath);

		Path inputputDir = new Path(inputPath);
		Path outputDir = new Path(outputPath);

		FileSystem fs = FileSystem.get(new Configuration());

		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapDF.class);
		job.setReducerClass(ReduceDF.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, inputputDir);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.waitForCompletion(true);

	}

	private static void runJob3(Job job, String inputPath, String outputPath)
			throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {

		System.out.println("Starting job 3...");
		System.out.println("Input: " + inputPath);
		System.out.println("output: " + outputPath);

		Path inputputDir = new Path(inputPath);
		Path outputDir = new Path(outputPath);

		FileSystem fs = FileSystem.get(new Configuration());

		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapTFIDF.class);
		job.setReducerClass(ReduceTFIDF.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, inputputDir);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.waitForCompletion(true);

	}

}
