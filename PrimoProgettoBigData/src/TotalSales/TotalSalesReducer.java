package TotalSales;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import TotalSales.Pair;

public class TotalSalesReducer extends
Reducer<Text, IntWritable, Text, IntWritable> {

	private PriorityQueue<Pair> queue;

	@Override
	protected void setup(Context ctx) {
		queue = new PriorityQueue<Pair>(new Comparator<Pair>() {
			public int compare(Pair p1, Pair p2) {
				return p1.sales.compareTo(p2.sales);
			}
		});
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		/* Incremento le vendite */
		int sales = 0;
		for (IntWritable value : values) {
			sales = sales + value.get();
		}
		/* Aggiungo la coppia alla coda ed elimino gli elementi eccedenti */
		queue.add(new Pair(key.toString(), sales));
	}

	/**
	 * Una volta terminato il task per questo reducer posso scrivere il
	 * risultato svuotando la coda
	 * 
	 */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		List<Pair> topPairs = new ArrayList<Pair>();
		while (!queue.isEmpty()) {
			topPairs.add(queue.remove());
		}
		/* Riestraggo gli elementi al contrario per avere il giusto ordinamento */
		for (int i = topPairs.size() - 1; i >= 0; i--) {
			Pair topKPair = topPairs.get(i);
			context.write(new Text(topKPair.couple), new IntWritable(
					topKPair.sales));
		}
	}
}