package TotalSales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalSales {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("USAGE: <hdfs input path> <hdfs output path>");
			System.exit(1);
		}
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TotalSales");
		
		job.setJarByClass(TotalSales.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(TotalSalesMapper.class);
		job.setReducerClass(TotalSalesReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean succ = job.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job failed");
		}
	}
}