package CouplesFrequency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CouplesFrequencyReducer extends
		Reducer<Text, ProductsListWritable, Text, DoubleWritable> {

	private static int TOP_K = 10;
	private PriorityQueue<Couple> queue;

	@Override
	protected void setup(Context ctx) {
		queue = new PriorityQueue<Couple>(TOP_K, new Comparator<Couple>() {
			public int compare(Couple c1, Couple c2) {
				return c1.percentage.compareTo(c2.percentage);
			}
		});
	}

	public void reduce(Text key, Iterable<ProductsListWritable> values,
			Context context) throws IOException, InterruptedException {

		double productTotalCount = 0;
		Map<String, Integer> productToOccurence = new HashMap<String, Integer>();
		for (ProductsListWritable value : values) {
			productTotalCount++;
			for (String product : value.getProductList()) {
				if (!product.isEmpty()) {
					if (!productToOccurence.containsKey(product)
							&& !product.isEmpty()) {
						productToOccurence.put(product, new Integer(1));
					} else {
						productToOccurence.put(product,
								productToOccurence.get(product) + 1);
					}
				}
			}
		}

		/*
		 * A questo punto per ogni prodotto trovato calcolo e scrivo la
		 * percentuale
		 */
		for (String product : productToOccurence.keySet()) {

			queue.add(new Couple(key.toString() + "," + product,
					productToOccurence.get(product) / productTotalCount));
			if (queue.size() > TOP_K) {
				queue.remove();
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		List<Couple> topKCouples = new ArrayList<Couple>();
		while (!queue.isEmpty()) {
			topKCouples.add(queue.remove());
		}
		/* Riestraggo gli elementi al contrario per avere il giusto ordinamento */
		for (int i = topKCouples.size() - 1; i >= 0; i--) {
			Couple topKCouple = topKCouples.get(i);
			context.write(new Text(topKCouple.couple), new DoubleWritable(
					topKCouple.percentage));
		}
	}

}