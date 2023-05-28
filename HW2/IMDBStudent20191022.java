import java.io.IOException;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20191022 {

	public static class Movie {
		public String title;
		public double rating;
		
		public Movie(String title, double rating) {
			this.title = title;
			this.rating = rating;
		}
		
		public String getTitle() {
			return title;
		}
		
		public Double getRating() {
			return rating;
		}
		
		public String getString() {
			return title + " " + rating;
		}
	}
	
	public static class MovieComparator implements Comparator<Movie> {
		public int compare(Movie x, Movie y) {
			if(x.rating > y.rating) return 1;
			if(x.rating < y.rating) return -1;
			return 0;
		}
	}
	
	public static void insertMovie(PriorityQueue q, String title, double rating, int topk) {
		Movie movie_head = (Movie)q.peek();
		
		if(q.size() < topk || movie_head.rating < rating) {
			Movie movie = new Movie(title, rating);
			q.add(movie);
			
			if(q.size() > topk) q.remove();
		}
	}

	public static class DoubleString implements WritableComparable
	{
		String joinKey = new String(); //MovieID
		String tableName = new String(); //data file
		
		public DoubleString(){}
		
		public DoubleString(String _joinKey, String _tableName)
		{
			joinKey = _joinKey;
			tableName = _tableName;
		}
		
		public void readFields(DataInput in) throws IOException
		{
			joinKey = in.readUTF();
			tableName = in.readUTF();
		}
		
		public void write(DataOutput out) throws IOException
		{
			out.writeUTF(joinKey);
			out.writeUTF(tableName);
		}
		
		public int compareTo(Object o1)
		{
			DoubleString o = (DoubleString) o1;
			
			int ret = joinKey.compareTo(o.joinKey);
			if(ret != 0) return ret;
			return -1 * tableName.compareTo(o.tableName);
		}
		
		public String toString()
		{
			return joinKey +" "+ tableName;
		}
	}
	
	public static class CompositeKeyComparator extends WritableComparator 
	{
		protected CompositeKeyComparator()
		{
			super(DoubleString.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			DoubleString k1 = (DoubleString) w1;
			DoubleString k2 = (DoubleString) w2;
			
			int result = k1.joinKey.compareTo(k2.joinKey);
			if(0 == result) {
				result = -1 * k1.tableName.compareTo(k2.tableName);
			}
			
			return result;	
		}	
	}
	
	public static class FirstPartitioner extends Partitioner<DoubleString, Text>
	{
		public int getPartition(DoubleString key, Text value, int numPartition)
		{
			return key.joinKey.hashCode()%numPartition;
		}
	}
	
	public static class FirstGroupingComparator extends WritableComparator
	{
		protected FirstGroupingComparator()
		{
			super(DoubleString.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			DoubleString k1 = (DoubleString) w1;
			DoubleString k2 = (DoubleString) w2;
			
			return k1.joinKey.compareTo(k2.joinKey);
		}
	}

	public static class IMDBMapper extends Mapper<Object, Text, DoubleString, Text>
	{
		boolean fileR = true;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] line = value.toString().split("::");
			DoubleString mapKey = null;
			Text mapValue = new Text();
			
			String val = "";

			if(fileR) {
				String movieID = line[1];
				String rating = line[2];
				
				//System.out.println(movieID);
				
				mapKey = new DoubleString(movieID, "R");
				val = "R|" +rating;
				
				mapValue.set(val);
				context.write(mapKey, mapValue);
				
			} else {
				String movieID = line[0];
				String movieTitle = line[1];
				String genre = line[2];
				
				//System.out.println(movieTitle);
				
				StringTokenizer itr = new StringTokenizer(genre, "|");
			
				while(itr.hasMoreTokens()){
					if(itr.nextToken().equals("Fantasy")) {
						mapKey = new DoubleString(movieID, "F");
						val = "F|" +movieTitle;
						
						mapValue.set(val);
						context.write(mapKey, mapValue);
					}
				}
			}
			
		}
		
		public void setup(Context context) throws IOException, InterruptedException 
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			
			//if(filename.indexOf("movies.txt") != -1) fileM = true;
			//else fileM = false;
			
			if(filename.indexOf("ratings.dat") != -1) fileR = true;
			else fileR = false;
		}
	}

	public static class IMDBReducer extends Reducer<DoubleString, Text, Text, DoubleWritable> 
	{
	
		private PriorityQueue<Movie> queue = new PriorityQueue<Movie>();
		private Comparator<Movie> comp = new MovieComparator();
		private int topk;

		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			//Text reduceKey = new Text();
			//DoubleWritable reduceValue = new DoubleWritable();
			
			String movieTitle = "";
			String rating = "";
			
			boolean flag = false;
			
			double sum = 0;
			int len = 0;
			double avg = 0;
			
			for(Text val: values) {
			
				//System.out.println(val.toString());
			
				StringTokenizer itr = new StringTokenizer(val.toString(), "|");
				String tableName = itr.nextToken();
				
				if(tableName.equals("R")) {
					rating = itr.nextToken();
					
					sum += Double.parseDouble(rating);
					len ++;
					
				} else {
					movieTitle = itr.nextToken();
					//System.out.println(movieTitle);
					//reduceKey.set(movieTitle);
					flag = true;
				}
			}
			
			if(flag) {
				avg = sum / len;
				//reduceValue.set(avg);	
				//context.write(reduceKey, reduceValue);
				
				insertMovie(queue, movieTitle, avg, topk);
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topk = conf.getInt("topk", -1);
			queue = new PriorityQueue<Movie>(topk, comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Movie movie = (Movie) queue.remove();
				context.write(new Text(movie.getTitle()), new DoubleWritable(movie.getRating()));
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
			System.err.println("Usage: IMDB <in> <out> <topk>");
			System.exit(2);
		}
		
		//conf.setInt("topk", topk);
		conf.setInt("topk", Integer.valueOf(otherArgs[2]));
		
		Job job = new Job(conf, "IMDBStudent20191022");
		
		job.setJarByClass(IMDBStudent20191022.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
