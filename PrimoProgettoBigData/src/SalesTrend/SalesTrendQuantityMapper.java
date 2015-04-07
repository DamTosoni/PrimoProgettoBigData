package SalesTrend;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesTrendQuantityMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private Text item = new Text();

	public static boolean isValidDate(String inDate) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		dateFormat.setLenient(false);
		try {
			dateFormat.parse(inDate.trim());
		} catch (ParseException pe) {
			return false;
		}
		return true;
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",");

		while (tokenizer.hasMoreTokens()) {
			item.set(tokenizer.nextToken());

			if (!(isValidDate(item.toString()))) {
				context.write(item, one);
			}
		}

	}

}