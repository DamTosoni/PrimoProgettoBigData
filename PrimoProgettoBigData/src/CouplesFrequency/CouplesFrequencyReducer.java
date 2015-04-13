package CouplesFrequency;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CouplesFrequencyReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		/* Incremento le vendite */
		int sales = 0;
		for (IntWritable value : values) {
			sales = sales + value.get();
		}
		context.write(key, new IntWritable(sales));
	}

}