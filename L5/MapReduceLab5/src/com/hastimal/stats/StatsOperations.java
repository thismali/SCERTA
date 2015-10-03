package com.hastimal.stats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * This class is used to perform Statistical Analysis on integer data set
 *
 */
public class StatsOperations {

	/**
	 * This Mapper class is used to Map input key/value pairs to a set of intermediate key/value pairs.
	 *
	 */
	public static class ReadInputMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		Text kword = new Text();
		LongWritable vword = new LongWritable();

		
		/* (non-Javadoc)This method reads integer dataset from source file and generate key/value pairs which are input to the reducer
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException
		{			
			String line = value.toString();
			String[] parts = line.split("\\n");
			long element = Long.valueOf(parts[0]);
			kword.set("ResultSet");
			vword.set(element);
			context.write(kword, vword);
		}
	}
	

	/**
	 * Reduces a set of intermediate values which share a key to a smaller set of values.
	 * This class is used for computation of count,sum,min,max,sum of individual squares from single source
	 *
	 */
	public static class IntermediateReducer extends Reducer<Text, LongWritable, Text, Text>
	{
		Text vword = new Text();
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
		
			long count = 0; 
			long min = Long.MAX_VALUE;
			long max = Long.MIN_VALUE;
			long sum = 0;
			long sumSquared = 0;
			double mean=0d;
			double variance =0d;
			double stDeviation =0d;
			double firstQuarterPercentile =0d;
            double secondQuarterPercentile =0d;
            double thirdQuarterPercentile =0d;
            List<Integer> cloneValues = new ArrayList<Integer>();
            long value;
            while (values.iterator().hasNext()) {
                ++count;
                value = values.iterator().next().get();
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                sumSquared += value * value;               
                cloneValues.add((int) value);
              }

            String listElements ="#InputNumbers:\t";
            for (Integer n:cloneValues){
            	listElements += n+"\t";
            }
            
            vword.set(listElements+"#Counter:\t" + count+"#"+"Sum:\t" + sum + "#MinVal:\t" + min+ "#MaxVal:\t" + max+ "#sumSquaredVal:\t" + sumSquared);
    			
			context.write(key, vword);
		}
	}
	/**
	 * This Class takes the merged file data and computes partial statistics on entire dataset.
	 *
	 */
	public static class ComputationMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text kword = new Text();
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException
		{
			System.out.println("\nValue is :"+value);				
			
			String line = value.toString();
			String[] words = line.split("#");
			String[] subElement;
			long n =0;
			long finalMax = Long.MIN_VALUE;
			long finalMin=Long.MAX_VALUE;
			long finalCount=0;
			long finalSum = 0;
			long finalSumSquaredVal =0;
			double mean =0d;
			double variance =0d;
			double stDeviation =0d;
			String finalElements = "";

			for(String element:words)
			{	
				 System.out.println("\nElement is :"+element);
				
					if(element.contains("ResultSet")){
						++n;
					}
					if(element.contains("InputNumbers:")){
						subElement = element.split("\\t");
						for(String str:subElement)
						{
						if(!str.equalsIgnoreCase("InputNumbers:"))
							finalElements+= Integer.valueOf(str)+"\t";
						}
					}
					if(element.contains("Counter:")){
						
						subElement = element.split("\\t");
						finalCount += Integer.valueOf(subElement[1]);
					}
					if(element.contains("Sum")){
						subElement = element.split("\\t");
						finalSum += Integer.valueOf(subElement[1]);
					}
					if(element.contains("MinVal")){
						subElement = element.split("\\t");
						finalMin=Math.min(finalMin, Integer.valueOf(subElement[1]));	
					}
					if(element.contains("MaxVal")){
						subElement = element.split("\\t");
						finalMax=Math.max(finalMax, Integer.valueOf(subElement[1]));				
					}
					if(element.contains("sumSquaredVal")){
						subElement = element.split("\\t");
						finalSumSquaredVal += Integer.valueOf(subElement[1]);				
					}
				
				}
          mean = (double) finalSum/finalCount;
          
          
  		
          variance = (finalSumSquaredVal - (finalSum * finalSum) / finalCount) / (finalCount - 1);
          
          stDeviation = Math.sqrt(variance);
			
          String map2Values ="#NumberofInputs\t"+n+"#Counter\t"+finalCount+"#Sum\t"+finalSum+"#MinVal\t"+finalMin+"#MaxVal\t"+finalMax+"#SumSquared\t"+finalSumSquaredVal+"#mean\t"+mean+"#variance\t"+variance+"#stDeviation\t"+stDeviation+"#InputNumbers\t"+finalElements;
          kword.set("Map2");
	
          context.write(kword, new Text (map2Values));
			
		}

		}
	
	/**
	 * This Class takes intermediate results from ComputationMapper and performs complete statistical analysis of entire integer dataset.
	 *
	 */
	public static class ComputationReducer extends Reducer<Text, Text, Text, Text>
	{
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
		//Local Variable declarations and Initialization
			String[] array ;
			String[] subElement;
			String finalResult="";
			String value;
			
			long  counter =0;
			long  sum=0;
			long  minVal=0;
			long  maxVal=0;
			double  mean=0d;
			double  stDeviation=0d;
			double firstQuarterPercentile =0d;
			double secondQuarterPercentile =0d;
			double thirdQuarterPercentile =0d;
			List<Integer> listofElements = new ArrayList<Integer>();
			
			
			while (values.iterator().hasNext()) {
             
				value = values.iterator().next().toString();
				array = value.split("#");
				
			for(String element:array)
			{		
				

		    	if(element.contains("Counter")){
					
					subElement = element.split("\\t");
					counter = Long.valueOf(subElement[1]);
				}
				if(element.contains("Sum")){
					subElement = element.split("\\t");
					sum = Long.valueOf(subElement[1]);
				}
				if(element.contains("MinVal")){
					subElement = element.split("\\t");
					minVal = Long.valueOf(subElement[1]);	
				}
				if(element.contains("MaxVal")){
					subElement = element.split("\\t");
					maxVal = Long.valueOf(subElement[1]);				
				}
				if(element.contains("mean")){
					subElement = element.split("\\t");
					mean = Double.valueOf(subElement[1]);				
				}
				if(element.contains("stDeviation")){
					subElement = element.split("\\t");
					stDeviation = Double.valueOf(subElement[1]);				
				}
				if(element.contains("InputNumbers")){
					subElement = element.split("\\t");
					for(String str:subElement)
					{
						
						if(!str.equalsIgnoreCase("InputNumbers"))
							
						listofElements.add(Integer.valueOf(str));
					}
				}
			}
		}
			

			Collections.sort(listofElements);
			
			firstQuarterPercentile= findPercentile(listofElements,0.25);
			secondQuarterPercentile=findPercentile(listofElements,0.50);
			thirdQuarterPercentile=findPercentile(listofElements,0.75);
		
	
            finalResult = "\nCOUNT:\t" + counter+ "\nMEAN:\t" + mean+"\nSTANDARD DEVIATION:\t"+stDeviation+ "\nMIN:\t" + minVal+ "\nMAX:\t" + maxVal+"\n25th PERCENTILE\t"+firstQuarterPercentile+"\n50th PERCENTILE\t"+secondQuarterPercentile+"\n75th PERCENTILE\t"+thirdQuarterPercentile;
    			
			context.write(new Text("StatisticalAnalysis"+"\n"+"----------------------------------"+"\n"), new Text(finalResult));

	}

		/**This method is used for calculating 25th,50th and 75th percentile for given set of integers
		 * @param data input integer list 
		 * @param percentile quarters
		 * @return percentile result
		 */
		private double findPercentile(List<Integer> data, double percentile) {
			double index = percentile*(data.size()+1);
		    int lower = (int)Math.floor(index);
		   
		    if(lower<0) { 
		       return data.get(0);
		    }
		    if(lower>data.size()-1) { 
		       return data.get(data.size()-1);
		    }
		    double fraction = index-lower;
		    // linear interpolation
		    double result=data.get(lower-1) + fraction*(data.get(lower)-data.get(lower-1));
		    
		    return result;
		}
		
	}
	
	
	/**
	 * This is method is used to configure jobs and execute them.
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		
		String readFile1;
		String readFile2;
		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(args.length != 4)
		{
			
			System.out.println("Usage is: hadoop jar jarname mainclass input1 input2 output");
			
			System.exit(1);
		}
	
		Job readFirstInputJob = new Job(conf, "Statistical Analysis of large dataset");
		Job readSecondInputJob = new Job(conf, "Statistical Analysis of large dataset2");
		Job computationJob = new Job(conf, "Statistical Analysis of large dataset3");

		readFirstInputJob.setJarByClass(StatsOperations.class);		
		readFirstInputJob.setMapperClass(ReadInputMapper.class);
		readFirstInputJob.setReducerClass(IntermediateReducer.class);
		readFirstInputJob.setMapOutputKeyClass(Text.class);
		readFirstInputJob.setMapOutputValueClass(LongWritable.class);
		readFirstInputJob.setOutputKeyClass(Text.class);
		readFirstInputJob.setOutputValueClass(Text.class);		
		readFirstInputJob.setNumReduceTasks(2);	
		readFirstInputJob.setInputFormatClass(TextInputFormat.class);
		readFirstInputJob.setOutputFormatClass(TextOutputFormat.class);		
		FileInputFormat.addInputPath(readFirstInputJob, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(readFirstInputJob, new Path("/user/biadmin/hackathon/Output"));		
		
		if (readFirstInputJob.waitForCompletion(true)){	
			
		readSecondInputJob.setJarByClass(StatsOperations.class);		
		readSecondInputJob.setMapperClass(ReadInputMapper.class);
		readSecondInputJob.setReducerClass(IntermediateReducer.class);
		readSecondInputJob.setMapOutputKeyClass(Text.class);
		readSecondInputJob.setMapOutputValueClass(LongWritable.class);
		readSecondInputJob.setOutputKeyClass(Text.class);
		readSecondInputJob.setOutputValueClass(Text.class);		
		readSecondInputJob.setNumReduceTasks(2);	
		readSecondInputJob.setInputFormatClass(TextInputFormat.class);
		readSecondInputJob.setOutputFormatClass(TextOutputFormat.class);		
		FileInputFormat.addInputPath(readSecondInputJob, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(readSecondInputJob, new Path("/user/biadmin/hackathon/Output2"));
		 
		if(readSecondInputJob.waitForCompletion(true)){
			Path out1= new Path("/user/biadmin/hackathon/Output/part-r-00000");
			Path out2= new Path("/user/biadmin/hackathon/Output2/part-r-00000");
			Path dest1 = new Path("/user/biadmin/hackathon/Output3/Output3.txt");
			Path dest2 = new Path("/user/biadmin/hackathon/Output3/Output4.txt");
			Path dest3 = new Path("/user/biadmin/hackathon/Output4/merged_data.txt");
			 
			 
			FileUtil.copy(hdfs,out1,hdfs,dest1,false,true,conf);
			FileUtil.copy(hdfs,out2,hdfs,dest2,false,true,conf);
			
			Thread.sleep(10000);
			
	        BufferedReader Output3=new BufferedReader(new InputStreamReader(hdfs.open(dest1)));
	        BufferedReader Output4=new BufferedReader(new InputStreamReader(hdfs.open(dest2)));
	        
	        BufferedWriter mergeOutput=new BufferedWriter(new OutputStreamWriter(hdfs.create(dest3,true)));
 
	        while (((readFile1  = Output3.readLine()) != null) && ((readFile2  = Output4.readLine()) != null))
		    {
		    	try {
		    		 
					mergeOutput.write(readFile1+"#"+readFile2);
					System.out.println("File Merge Completed");
		 
				} catch (Exception e) {
					e.printStackTrace();
				}
		    }
		    mergeOutput.close();
		    	
			Thread.sleep(20000);
			
			computationJob.setJarByClass(StatsOperations.class);		
			computationJob.setMapperClass(ComputationMapper.class);
			computationJob.setReducerClass(ComputationReducer.class);
			computationJob.setMapOutputKeyClass(Text.class);
			computationJob.setMapOutputValueClass(Text.class);
			computationJob.setOutputKeyClass(Text.class);
			computationJob.setOutputValueClass(Text.class);		
			computationJob.setNumReduceTasks(2);
			computationJob.setInputFormatClass(TextInputFormat.class);
			computationJob.setOutputFormatClass(TextOutputFormat.class);		
			
			FileInputFormat.addInputPath(computationJob, new Path("/user/biadmin/hackathon/Output4/merged_data.txt"));
			FileOutputFormat.setOutputPath(computationJob, new Path(otherArgs[3]));
			
			System.exit(computationJob.waitForCompletion(true) ? 0 : 1);
		}
		
		}
		else{
		System.exit(readFirstInputJob.waitForCompletion(true) ? 0 : 1);
		}
	}
}