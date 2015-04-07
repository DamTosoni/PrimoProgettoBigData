package SalesTrend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesTrendRankerReducer extends
Reducer<Text, IntWritable, Text, IntWritable> {

	private class Pair {
		public String item;
		public Integer quantity;

		public Pair(String item, Integer quantity) {
			this.item = item;
			this.quantity = quantity;
		}
	};
	private PriorityQueue<Pair> queue;

	@Override
	protected void setup(Context ctx) {
		queue = new PriorityQueue<Pair>(new Comparator<Pair>() {
			public int compare(Pair p1, Pair p2) {
				return p1.quantity.compareTo(p2.quantity);
			}
		});
	}

	@Override
	protected void cleanup(Context ctx) 
			throws IOException, InterruptedException {
		List<Pair> topKPairs = new ArrayList<Pair>();
		while (!queue.isEmpty()) {
			topKPairs.add(queue.remove());
		}
		for (int i = topKPairs.size() - 1; i >= 0; i--) {
			Pair topKPair = topKPairs.get(i);
			ctx.write(new Text(topKPair.item), 
					new IntWritable(topKPair.quantity));
		}
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {

		int count = 0;
		for (IntWritable value : values) {
			count = count + value.get();
		}
		queue.add(new Pair(key.toString(), count));
	}
}