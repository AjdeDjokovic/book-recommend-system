package apriori;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FreqItemSet {
	public static class FreqItemSetMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] parts = line.split("\t");
			
			for(int i = 0;i < parts.length;i++)
				context.write(new Text(parts[i]), one);
		}
	}
	
	public static class FreqItemSetCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value:values)
				sum += value.get();
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class FreqItemSetReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
		
	    Job job = new Job(conf, "FreqItemSet");
	    job.setJarByClass(FreqItemSet.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setMapperClass(FreqItemSetMapper.class);
	    job.setCombinerClass(FreqItemSetCombiner.class);
	    job.setReducerClass(FreqItemSetReducer.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	}
}
