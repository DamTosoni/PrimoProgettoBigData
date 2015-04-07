package SalesTrend;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesTrendRankerMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {
			context.write(new Text(tokenizer.nextToken()), new IntWritable(Integer.parseInt(tokenizer.nextToken())));
		}

	}

}