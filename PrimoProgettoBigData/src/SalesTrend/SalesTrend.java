package SalesTrend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesTrend {

	public static void main(String[] args) throws Exception {

		Path input = new Path(args[0]);
		Path temp1 = new Path("temp");
		Path output = new Path(args[1]);


		//Calcolo delle vendite per ogni prodotto
		Job job1 = new Job(new Configuration(), "SalesQuantities");

		job1.setJarByClass(SalesTrend.class);

		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, temp1);

		job1.setMapperClass(SalesTrendQuantityMapper.class);
		job1.setReducerClass(SalesTrendQuantityReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		boolean succ = job1.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job1 failed");
		}

		//Ordinamento decrescente
		Job job2= new Job(new Configuration(), "SalesRanker");

		job2.setJarByClass(SalesTrend.class);

		FileInputFormat.addInputPath(job2, temp1);
		FileOutputFormat.setOutputPath(job2, output);

		job2.setMapperClass(SalesTrendRankerMapper.class);
		job2.setReducerClass(SalesTrendRankerReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		succ = job2.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job2 failed");
		}
	}
}