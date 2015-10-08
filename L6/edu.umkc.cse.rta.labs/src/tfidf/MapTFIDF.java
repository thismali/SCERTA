package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MapTFIDF extends Mapper<LongWritable, Text, Text, Text> {

	private Text wordkey = new Text();
	private Text docFrequency = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] words = value.toString().split("\t");

		int seperator = words[0].indexOf("@");

		wordkey.set(words[0].substring(0, seperator));
		docFrequency.set(words[0].substring(seperator + 1) + ":" + words[1]);

		context.write(wordkey, docFrequency);

	}
}
