package LessThan5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LessThan5 {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		if (args.length != 2) {
			System.err.println("USAGE: <hdfs input path> <hdfs output path>");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "LessThan5");

		job.setJarByClass(LessThan5.class);
		job.setMapperClass(LessThan5Mapper.class);
		job.setCombinerClass(LessThan5Reducer.class);

		job.setNumReduceTasks(1); // un solo reducer per evitare che ognuno calcoli la sua top 10
		job.setReducerClass(LessThan5Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
		} catch (IllegalArgumentException | IOException e) {
			System.err.println("Error opening input path");
			throw e;
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}