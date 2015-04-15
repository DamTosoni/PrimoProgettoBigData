package CouplesFrequency;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (<prodotto1,prodotto2>,1) e le invia al
 * reducer per effettuare il conteggio
 *
 */
public class CouplesFrequencyMapper extends
		Mapper<Object, Text, Text, ProductsListWritable> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */
		String[] values = value.toString().split(",");

		/* Tolgo il primo elemento (ossia la data) */
		String[] products = Arrays.copyOfRange(values, 1, values.length);

		for (String product : products) {
			/*
			 * Per ogni prodotto costruisco la lista di prodotti venduti insieme
			 * ad esso
			 */
			CoupleProductOccurrence[] productList = new CoupleProductOccurrence[products.length];
			productList[0] = new CoupleProductOccurrence("TOTALROWS", 1);
			int i = 1;
			for (String product2 : products) {
				if (!product.equals(product2)) {
					productList[i] = new CoupleProductOccurrence(product2, 1);
					i++;
				}
			}
			ProductsListWritable productsListWritable = new ProductsListWritable(
					productList);
			context.write(new Text(product), productsListWritable);
		}
	}
}