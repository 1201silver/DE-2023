import java.io.IOException;
import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20191022 {

	public static class UBERMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text uberKey = new Text();
		private Text uberValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			
			String region = itr.nextToken().trim();
			String date = itr.nextToken().trim();
			
			String trips = itr.nextToken().trim();
			String vehicles = itr.nextToken().trim();
			
			String[] week = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
			String day = "";
			
			try {
				SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy");
				Date d = df.parse(date);
				
				Calendar cal = Calendar.getInstance();
				cal.setTime(d);
				
				int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK) - 1;
				day = week[dayOfWeek];
				
			} catch(Exception e) {
				e.printStackTrace();
			}
		
			uberKey.set(region +","+ day);
			uberValue.set(trips +","+ vehicles);
			
			context.write(uberKey, uberValue);		
		}
	}

	public static class UBERReducer extends Reducer<Text, Text, Text, Text> 
	{
		private Text sumValue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{		
			int sumT = 0;
			int sumV = 0;
			
			for (Text val : values) 
			{
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				
				int trips = Integer.parseInt(itr.nextToken().trim());
				int vehicles = Integer.parseInt(itr.nextToken().trim());
				
				sumT += trips;
				sumV += vehicles;
			
			}
			sumValue.set(sumT +","+ sumV);
			context.write(key, sumValue);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: UBER <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "UBERStudent20191022");
		
		job.setJarByClass(UBERStudent20191022.class);
		job.setMapperClass(UBERMapper.class);
		job.setReducerClass(UBERReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
    		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
