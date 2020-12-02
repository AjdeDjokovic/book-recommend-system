package three;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountRate {
	public static class CountRateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private String pattern = "[^\\w]";

		
		/*
		 * input:					value:User-ID;"ISBN";"Book-Rating"
		 * output:	key:ISBN		value:Book-Rating
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			line = line.replaceAll(pattern, " ");

			String[] parts = line.trim().split("\\s+");


			if (parts.length == 3 && parts[0].charAt(0) != 'U' && parts[1].length() == 10) {
				String user = parts[0];
				String isbn = parts[1];


				int rate = Integer.parseInt(parts[2]);

				context.write(new Text(isbn), new IntWritable(rate));
			}
		}
	}

	public static class CountRateCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

	public static class CountRateReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		private HashMap<String,Integer> freq = new HashMap<String,Integer>();

		/*
		 * get	key:ISBN	value:sum
		 */
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			Path path = new Path("hdfs://localhost:9000/user/201700301147/apriori2/FreqItemSetOutput/part-r-00000");
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(path);
			BufferedReader d = new BufferedReader(new InputStreamReader(in));

			String line;
			while ((line = d.readLine()) != null) {
				String[] parts = line.split("\t");
				freq.put(parts[0], Integer.parseInt(parts[1]));
			}

			d.close();
			in.close();
		}

		/*
		 * input:	key:ISBN		value:Book-Rating
		 * output:	key:ISBN		value:avg-Rate
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			
			for (IntWritable value : values) {
				sum += value.get();
			}
			
			double count = (double) freq.get(key.toString());
			
			double rate = (double)sum / count;

			context.write(key, new DoubleWritable(rate));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1])))
			fs.delete(new Path(args[1]), true);

		Job job = new Job(conf, "CountRate");
		job.setJarByClass(CountRate.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(CountRateMapper.class);
		job.setCombinerClass(CountRateCombiner.class);
		job.setReducerClass(CountRateReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
