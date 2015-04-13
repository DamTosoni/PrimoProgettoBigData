package SalesTrend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesTrendReducer extends
		Reducer<Text, TrimesterWritable, Text, TrimesterWritable> {

	public void reduce(Text key, Iterable<TrimesterWritable> values,
			Context context) throws IOException, InterruptedException {

		int firstMonthSales = 0, secondMonthSales = 0, thirdMonthSales = 0;

		for (TrimesterWritable value : values) {
			firstMonthSales += value.getFirstMonth().get();
			secondMonthSales += value.getSecondMonth().get();
			thirdMonthSales += value.getThirdMonth().get();
		}
		context.write(key, new TrimesterWritable(new IntWritable(
				firstMonthSales), new IntWritable(secondMonthSales),
				new IntWritable(thirdMonthSales)));
	}

}