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
	private PriorityQueue<CouplePercentage> queue;

	@Override
	protected void setup(Context ctx) {
		queue = new PriorityQueue<CouplePercentage>(TOP_K,
				new Comparator<CouplePercentage>() {
					public int compare(CouplePercentage c1, CouplePercentage c2) {
						return c1.percentage.compareTo(c2.percentage);
					}
				});
	}

	public void reduce(Text key, Iterable<ProductsListWritable> values,
			Context context) throws IOException, InterruptedException {

		double productTotalCount = 0;
		Map<String, Integer> productToOccurence = new HashMap<String, Integer>();
		for (ProductsListWritable value : values) {
			for (CoupleProductOccurrence product : value.getProductList()) {
				if (!product.getProduct().equals("TOTALROWS")) {
					if (!productToOccurence.containsKey(product)) {
						productToOccurence.put(product.getProduct(),
								new Integer(product.getOccurrence()));
					} else {
						productToOccurence.put(product.getProduct(),
								productToOccurence.get(product) + 1);
					}
				} else {
					productTotalCount += product.getOccurrence();
				}
			}
		}

		/*
		 * A questo punto per ogni prodotto trovato calcolo e scrivo la
		 * percentuale
		 */
		for (String product : productToOccurence.keySet()) {

			queue.add(new CouplePercentage(key.toString() + "," + product,
					productToOccurence.get(product) / productTotalCount));
			if (queue.size() > TOP_K) {
				queue.remove();
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		List<CouplePercentage> topKCouples = new ArrayList<CouplePercentage>();
		while (!queue.isEmpty()) {
			topKCouples.add(queue.remove());
		}
		/* Riestraggo gli elementi al contrario per avere il giusto ordinamento */
		for (int i = topKCouples.size() - 1; i >= 0; i--) {
			CouplePercentage topKCouple = topKCouples.get(i);
			context.write(new Text(topKCouple.couple), new DoubleWritable(
					topKCouple.percentage));
		}
	}

}