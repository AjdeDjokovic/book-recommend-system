package apriori;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FreqItemSet2 {
	public static class FreqItemSet2Mapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private IntWritable one = new IntWritable(1);
//		public HashMap<String,Integer> freq = new HashMap<String,Integer>();
//		
//		public void setup(Context context) throws IOException
//		{
//			Configuration conf = context.getConfiguration();
//			Path path = new Path( "hdfs://localhost:9000/user/201700301147/apriori/FreqItemSetOutput/part-r-00000");
//			FileSystem fs = FileSystem.get(conf);
//			FSDataInputStream in = fs.open(path);
//			BufferedReader d = new BufferedReader(new InputStreamReader(in));
//			
//			String line;
//			while ((line = d.readLine()) != null) {
//				String[] res = line.split("\t");
//				freq.put(res[0], new Integer(res[1]));
//			}
//			
//			d.close();
//			in.close();
//		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] parts = line.split("\t");
			
			int length = parts.length;
			for(int i = 0;i < length;i++)
				for(int j = i + 1;j < length;j++)
				{
					context.write(new Text(parts[i] + ";" + parts[j]), one);
					context.write(new Text(parts[j] + ";" + parts[i]), one);
				}
		}
	}
	
	public static class FreqItemSet2Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value:values)
				sum += value.get();
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class FreqItemSet2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value:values)
				sum += value.get();
			if(sum > 0)
				context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1])))
			fs.delete(new Path(args[1]), true);
		
	    Job job = new Job(conf, "FreqItemSet2");
	    job.setJarByClass(FreqItemSet2.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setMapperClass(FreqItemSet2Mapper.class);
	    job.setCombinerClass(FreqItemSet2Combiner.class);
	    job.setReducerClass(FreqItemSet2Reducer.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	}

}
