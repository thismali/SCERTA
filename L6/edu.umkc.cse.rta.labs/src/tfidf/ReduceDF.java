package tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceDF extends Reducer<Text, Text, Text, Text> {

	MapWritable map = new MapWritable();
	Text outputKey = new Text();
	Text outputValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int dWordCount = 0;
		Map<String, Integer> map = new HashMap<String, Integer>();

		for (Text val : values) {
			outputKey.set(val.toString().substring(0,
					val.toString().indexOf(":")));
			String count = val.toString().substring(
					val.toString().indexOf(":") + 1);
			dWordCount += Integer.parseInt(count);

			map.put(outputKey.toString(), Integer.parseInt(count));

		}

		for (String thisKey : map.keySet()) {
			outputKey.set(key.toString() + "@" + thisKey);
			outputValue.set(map.get(thisKey) + "/" + dWordCount);

			context.write(outputKey, outputValue);
		}
	}

}
