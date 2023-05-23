import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20191022 {

	public static class YouTube {
		public String category;
		public double rating;
		
		public YouTube(String category, double rating) {
			this.category = category;
			this.rating = rating;
		}
		
		public String getCategory() {
			return category;
		}
		
		public Double getRating() {
			return rating;
		}
	}	
	
	public static class YouTubeComparator implements Comparator<YouTube> {
		public int compare(YouTube x, YouTube y) {
			if(x.rating > y.rating) return 1;
			if(x.rating < y.rating) return -1;
			return 0;
		}
	}
	
	public static void insertEmp(PriorityQueue q, String category, double rating, int topk) {
	
		YouTube yt_head = (YouTube)q.peek();
		
		if(q.size() < topk || yt_head.rating < rating) {
			YouTube yt = new YouTube(category, rating);
			q.add(yt);
			
			if(q.size() > topk) q.remove();
		}
	}

	public static class YouTubeMapper extends Mapper<Object, Text, Text, DoubleWritable>
	{
		private PriorityQueue<YouTube> queue;
		private Comparator<YouTube> comp = new YouTubeComparator();
		private Text ytKey = new Text();
		private DoubleWritable ytValue = new DoubleWritable();
		private int topk;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			/*String[] line = value.toString().split("|");
			
			String category = line[3];
			double rating = Double.parseDouble(line[6]);*/
			
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			itr.nextToken();
			itr.nextToken();
			itr.nextToken();
			String category = itr.nextToken();
			itr.nextToken();
			itr.nextToken();
			double rating = Double.parseDouble(itr.nextToken());
			
			
			ytKey.set(category);
			ytValue.set(rating);
			
			context.write(ytKey, ytValue);
		}
	}

	public static class YouTubeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> 
	{
		private PriorityQueue<YouTube> queue = new PriorityQueue<YouTube>();
		private Comparator<YouTube> comp = new YouTubeComparator();
		private int topk;

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
		{	
			int size = 0;
			double sum = 0;
			for(DoubleWritable val: values){
				sum += val.get();
				size++;
			}	
			double rating = sum / size;
			
			insertEmp(queue, key.toString(), rating, topk);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topk = conf.getInt("topk", -1);
			queue = new PriorityQueue<YouTube>(topk, comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				YouTube yt = (YouTube) queue.remove();
				context.write(new Text(yt.getCategory()), new DoubleWritable(yt.getRating()));
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		//int topk = 3;
		
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: YouTube <in> <out> <topk>");
			System.exit(2);
		}
		
		//conf.setInt("topk", topk);
		conf.setInt("topk", Integer.valueOf(otherArgs[2]));
		
		Job job = new Job(conf, "YouTubeStudent20191022");
		
		job.setJarByClass(YouTubeStudent20191022.class);
		job.setMapperClass(YouTubeMapper.class);
		job.setCombinerClass(YouTubeReducer.class);
		job.setReducerClass(YouTubeReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
