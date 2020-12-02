package three;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountCL {
	public static class CountCLMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		
		/*
		 * input:					value:ISBN \t sum
		 * output:	key:ISBN		value:sum
		 * 
		 * input:					value:ISBN;ISBN \t sum
		 * output:	key:ISBN		value:ISBN;ISBN \t sum
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] parts = line.split("\t");
			String[] tuple = parts[0].split(";");
			
			if(tuple.length == 2)
			{
				context.write(new Text(tuple[0]), value);
			}
			else if(tuple.length == 1)
			{
				context.write(new Text(parts[0]), new Text(parts[1]));
			}
		}
	}
	
	/*
	 * input:	key:ISBN			value:sum
	 * 			key:ISBN			value:ISBN;ISBN \t sum
	 * output:	key:ISBN \t ISBN	value:CL
	 */
	public static class CountCLReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			List<String> itemSet = new ArrayList<String>();
			double one = 0;
			for(Text value:values)
			{
				String line = value.toString();
				String[] parts = line.split("\t");
				if(parts.length == 2)
				{
					itemSet.add(line);
				}
				else if(parts.length == 1)
				{
					one = Double.parseDouble(parts[0]);
				}
			}
			
			if(itemSet.size() >= 1)
			{
				for(String item:itemSet)
				{
					String[] parts = item.toString().split("\t");
					double two = Double.parseDouble(parts[1]);
					double res = two / one;
					
					if(res >= 0.10)
					{
						String[] tuple = parts[0].split(";");
						context.write(new Text(tuple[0] + "\t" + tuple[1]), new DoubleWritable(res));
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[2])))
			fs.delete(new Path(args[2]), true);
		
	    Job job = new Job(conf, "CountCL");
	    job.setJarByClass(CountCL.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    job.setMapperClass(CountCLMapper.class);
	    job.setReducerClass(CountCLReducer.class);
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]),new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    
	    job.waitForCompletion(true);
	}
}
