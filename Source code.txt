
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Question4 {
	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		HashSet<String> set = new HashSet<String>();

		protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = new Configuration();
			FileSystem filesystem = FileSystem.get(conf);
			URI[] paths = context.getCacheFiles();
			Path path = new Path(paths[0].getPath());
			readFile(filesystem.open(path));

		}

		public void readFile(FSDataInputStream path) throws IOException {
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(path));
			String line = bufferedReader.readLine();
			while (line != null) {

				String[] data = line.split("::");
				if (data.length >= 2 && data[1].contains("Palo Alto")) {

					set.add(data[0]);
				}
				line = bufferedReader.readLine();
			}
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
						throws IOException, InterruptedException {
			String[] line = value.toString().split("::");

			if (line.length >= 4 && set.contains(line[2])) {

				context.write(new Text(line[1]), new DoubleWritable(Double.parseDouble(line[3])));
			}
		}
	}

	static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
						throws IOException, InterruptedException {

			double sum = 0.0;
			int count = 0;
			for (DoubleWritable value : values) {
				sum += value.get();
				count++;
			}
			double avg_rating = sum / count;
			context.write(key, new DoubleWritable(avg_rating));
		}

	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: Mutual Friend <inputfile hdfs path for business.csv> <input file hdfs path for review.csv>  <output file hdfs path>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "UserId and Average Ratings");
		
		job.addCacheFile(new Path(args[0]).toUri());
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setJarByClass(Question4.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}