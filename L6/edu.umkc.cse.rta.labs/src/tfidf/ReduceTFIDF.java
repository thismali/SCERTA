package tfidf;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceTFIDF extends Reducer<Text, Text, Text, Text> {

	MapWritable map = new MapWritable();
	Text outputKey = new Text();
	Text outputValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		//here we get the corpus (number of docs in the input directory)
		int Corpus = new File(TFIDF.corpusPath).listFiles().length;
		
		
		int dWordCount = 0;
		int totalWordCount = 0;

		Map<String, String> map = new HashMap<String, String>();

		int TermCorpusCounter = 0;
		for (Text val : values) {
			outputKey.set(val.toString().substring(0,
					val.toString().indexOf(":")));
			String counts = val.toString().substring(
					val.toString().indexOf(":") + 1);
			dWordCount += Integer.parseInt(counts.split("/")[0]);
			totalWordCount += Integer.parseInt(counts.split("/")[1]);

			if (dWordCount > 0)
				TermCorpusCounter++;

			map.put(outputKey.toString(), counts);

		}

		for (String thisKey : map.keySet()) {

			String value = map.get(thisKey);
			dWordCount = Integer
					.parseInt(value.substring(0, value.indexOf("/")));
			totalWordCount = Integer.parseInt(value.substring(value
					.indexOf("/") + 1));
			
			//tf*idf computation
			double tf = Double.valueOf(Double.valueOf(dWordCount)
					/ Double.valueOf(totalWordCount)); // this is the term
														// frequency
			
			double idf = Math
					.log10((double) Corpus
							/ (double) (TermCorpusCounter == 0 ? 1
									: TermCorpusCounter));

			double tfIdf = tf * idf;

			DecimalFormat myFormatter = new DecimalFormat(
					"#,###,###.############");
			outputKey.set(key.toString() + "@" + thisKey);
			outputValue.set(myFormatter.format(tfIdf));

			context.write(outputKey, outputValue);
		}
	}

}
