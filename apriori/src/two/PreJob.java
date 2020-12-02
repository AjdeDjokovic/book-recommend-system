package two;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
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

public class PreJob {
	public static class PreJobMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		private String pattern = "[^\\w]";
		private final static int low = 0;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
//			System.out.println(line);
			
//			for(int i = 0;i < line.length();i++)
//				System.out.print(" " + (int)line.charAt(i));
//			System.out.println();
			
			line = line.replaceAll(pattern, " ");
			
			String[] parts = line.trim().split("\\s+");
			
//			System.out.println(parts.length);
			
			if(parts.length == 3 && parts[0].charAt(0) != 'U' && parts[1].length() == 10)
			{
				String user = parts[0];
				String isbn = parts[1];
				
//				System.out.println(parts[0]);
				
				Double rate = Double.parseDouble(parts[2]);
				
				if(rate >= low)
					context.write(new Text(user), new Text(isbn));
			}
		}
	}
	
	public static class PreJobReducer extends Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
		{
			String out = "";
			int blank = 0;
			
			for(Text value:values)
			{
				if(blank++ != 0)
					out += "\t";
				
				String isbn = value.toString();
				out += isbn;
			}
			
			context.write(new Text(out), NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1])))
			fs.delete(new Path(args[1]), true);
		
	    Job job = new Job(conf, "PreJob");
	    job.setJarByClass(PreJob.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    job.setMapperClass(PreJobMapper.class);
	    job.setReducerClass(PreJobReducer.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	}

}
