package tfidf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.*;

public class MapTF extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private IntWritable frequency =  new IntWritable(1);
	private Text term = new Text();
	//private Text N = new Text("N");

	@Override
	protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] words = value.toString().split(" ");	
		
		System.out.println("Words: "+ words.length);
		
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		for(String word: words)
		{
			word = word.replace("^[a-zA-Z]", "");
			
			if(StringUtils.isBlank(word)) continue;
			
			term.set(String.format("%s@%s", word, fileName));
			
			//frequency.put(term, new IntWritable(1)); //term frequency
			//frequency.put(N, new IntWritable(1));// running total
			
			context.write(term, frequency);
			
		}
	}
}
