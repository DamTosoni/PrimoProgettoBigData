package CouplesFrequency;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CouplesFrequencyCombiner extends
		Reducer<Text, ProductsListWritable, Text, ProductsListWritable> {

	public void reduce(Text key, Iterable<ProductsListWritable> values,
			Context context) throws IOException, InterruptedException {

		Map<String, Integer> productToOccurence = new HashMap<String, Integer>();
		for (ProductsListWritable value : values) {
			for (CoupleProductOccurrence product : value.getProductList()) {

				if (!productToOccurence.containsKey(product.getProduct())) {
					productToOccurence.put(product.getProduct(), new Integer(
							product.getOccurrence()));
				} else {
					productToOccurence.put(
							product.getProduct(),
							productToOccurence.get(product.getProduct())
									+ product.getOccurrence());
				}
			}
		}

		/*
		 * A questo punto scrivo l'output per le occorrenze trovate
		 */
		CoupleProductOccurrence[] productsList = new CoupleProductOccurrence[productToOccurence
				.keySet().size()];
		int i = 0;
		for (String product : productToOccurence.keySet()) {
			productsList[i] = new CoupleProductOccurrence(product,
					productToOccurence.get(product));
			i++;
		}
		context.write(key, new ProductsListWritable(productsList));
	}
}