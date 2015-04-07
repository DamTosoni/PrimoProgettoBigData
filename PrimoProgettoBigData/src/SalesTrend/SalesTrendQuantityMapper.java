package SalesTrend;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesTrendQuantityMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private Text item = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",");

		while (tokenizer.hasMoreTokens()) {
			item.set(tokenizer.nextToken());
			
			//TODO Togliere le date!!!

			//if (!item.toString().startsWith("2")) {
				context.write(item, one);
			//}
		}

	}

}