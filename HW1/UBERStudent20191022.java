import java.io.IOException;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;

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
		
		String d = "";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
		
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			
			String region = itr.nextToken();
			String date = itr.nextToken();
		
			StringTokenizer itr2 = new StringTokenizer(date, "/");
		
			while(itr2.hasMoreTokens()) {
				int month = Integer.parseInt(itr2.nextToken());
				int day = Integer.parseInt(itr2.nextToken());
				int year = Integer.parseInt(itr2.nextToken());
				
				LocalDate ld = LocalDate.of(year, month, day);
				DayOfWeek dow = ld.getDayOfWeek();
				int dayOfWeek = dow.getValue();
				
				switch(dayOfWeek) {
					case 1:
						d = "MON";
						break;
					case 2:
						d = "TUE";
						break;
					case 3:
						d = "WED";
						break;
					case 4:
						d = "THR";
						break;
					case 5:
						d = "FRI";
						break;
					case 6:
						d = "SAT";
						break;
					case 7:
						d = "SUN";
						break;
					default:
						d = "error";
						break;
				}
				
			}
			
			
			String vehicles = itr.nextToken();
			String trips = itr.nextToken();
			
			uberKey.set(region +","+ d);
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
				String data[] = (val.toString().split(","));
				sumT += Integer.parseInt(data[0]);
				sumV += Integer.parseInt(data[1]);
			
			}
			sumValue.set(String.valueOf(sumT) +","+ String.valueOf(sumV));
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
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
