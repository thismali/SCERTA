package com.hastimal.LabRealTimeBigData;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StatisticsOperations {
	/*3) MapReduce & Spark Programming for simple statistic operations 
	 * (e.g. mean/median/mode/range/variance/standard deviation)*/
	/**
	 * http://codereview.stackexchange.com/questions/42885/calculate-min-max-average-and-variance-on-a-large-dataset
	 */

	public static class StatisticsOperationsMapper  extends
	Mapper<LongWritable, Text, Text, MapWritable> {
		Text countKey = new Text("Total Numbers");
		Text maxKey = new Text("Maximum among Numbers");
		Text minKey = new Text("Minimum among Numbers");
		Text sumKey = new Text("Sum of Numbers");
		Text textKey = new Text("1");

		MapWritable mw = new MapWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			int number = Integer.parseInt(line);
			IntWritable num = new IntWritable(number);

			mw.put(countKey, new IntWritable(1));
			mw.put(maxKey, new IntWritable(number));
			mw.put(minKey, new IntWritable(number));
			mw.put(sumKey, new IntWritable(number));

			context.write(textKey, mw);
			
		}
		
	}

	public static class StatisticsOperationsReducer extends
	Reducer<Text, MapWritable, Text, FloatWritable> {

		Text countKey = new Text("Total Numbers");
		Text maxKey = new Text("Maximum among Numbers");
		Text minKey = new Text("Minimum among Numbers");
		Text sumKey = new Text("Sum of Numbers");
		Text textKey = new Text("1");
		MapWritable mw = new MapWritable();

		@Override
		protected void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {

			MapWritable firstMapWritable = values.iterator().next();
			int max = ((IntWritable) firstMapWritable.get(maxKey)).get();
			int min = ((IntWritable) firstMapWritable.get(minKey)).get();
			int count = ((IntWritable) firstMapWritable.get(countKey)).get();
			int number = ((IntWritable) firstMapWritable.get(sumKey)).get();

			int sum = number;

			int mean = 0;
			int M2 = 0;
			int delta = number - mean;
			mean = mean + delta / count;
			M2 += delta * (number - mean);

			for (MapWritable m : values) {
				IntWritable sumWritable = (IntWritable) m.get(sumKey);
				IntWritable countIntWritable = (IntWritable) m.get(countKey);
				// Calculating Standard deviation using algorithm proposed by Donald E. Knuth
				delta = number - mean;
				mean = mean + delta / count;
				M2 += delta * (number - mean);

				count += countIntWritable.get();

				IntWritable maxWritable = (IntWritable) m.get(maxKey);
				max = Math.max(maxWritable.get(), max);

				IntWritable minWritable = (IntWritable) m.get(minKey);
				min = Math.min(minWritable.get(), min);

				number = sumWritable.get();
				sum += number;
			}

			context.write(countKey, new FloatWritable(count));
			context.write(maxKey, new FloatWritable(max));
			context.write(minKey, new FloatWritable(min));
			context.write(sumKey, new FloatWritable(sum));
			float finalMean = (float) sum / count;
			context.write(new Text("Mean of Numbers"), new FloatWritable(finalMean));
			
			//Standard Deviation is just the square root of Variance
			double sd = Math.sqrt((float) M2 / (count - 1));
			double var = sd*sd;
			context.write(new Text("Variance"), new FloatWritable((float) var));
			context.write(new Text("Standard Deviation"), new FloatWritable((float) sd));
			

		}
	}

	
	public static void main(String[] args) throws IllegalArgumentException,
	IOException, ClassNotFoundException, InterruptedException {

		//input and output path
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration();

		// Create Job
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "StatisticsOperations");

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		// Setup MapReduce
		job.setMapperClass(StatisticsOperationsMapper.class);
		job.setReducerClass(StatisticsOperationsReducer.class);
		job.setJarByClass(StatisticsOperations.class);

		//input output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//set number of reducer
		job.setNumReduceTasks(1);

		//input output path
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		//job.waitForCompletion(true);
		System.out.println("##################Job is Done successfully##################!!!");

		//Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}
}