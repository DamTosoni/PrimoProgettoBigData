package SalesTrend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesTrend {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		if (args.length != 2) {
			System.err.println("USAGE: <hdfs input path> <hdfs output path>");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sales Trend");

		job.setJarByClass(SalesTrend.class);
		job.setMapperClass(SalesTrendMapper.class);
		job.setCombinerClass(SalesTrendReducer.class);

		job.setReducerClass(SalesTrendReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TrimesterWritable.class);

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