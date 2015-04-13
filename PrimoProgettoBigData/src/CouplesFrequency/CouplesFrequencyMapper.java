package CouplesFrequency;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (<prodotto1,prodotto2>,1) e le invia al
 * reducer per effettuare il conteggio
 *
 */
public class CouplesFrequencyMapper extends
		Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */
		String[] values = value.toString().split(",");
		if (values.length > 2) {
			/*
			 * Se ho almeno due elementi (piu' la data) calcolo il risultato,
			 * altrimenti escludo la riga
			 */

			/* Tolgo il primo elemento (ossia la data) */
			String[] products = Arrays.copyOfRange(values, 1, values.length);

			/* A questo punto creo le possibili permutazioni di coppie */
			List<String> couples = buildCouples(products);

			/* Scrivo gli elementi */
			for (String couple : couples) {
				context.write(new Text(couple), one);
			}
		}
	}

	/**
	 * Questo metodo di supporto costruisce una lista di coppie di stringhe a
	 * partire da un insieme di prodotti
	 * 
	 * @param products
	 *            Array contenente i prodotti
	 * @return Una lista di coppie di prodotti
	 */
	private List<String> buildCouples(String[] products) {
		int i, j;
		List<String> couples = new LinkedList<String>();
		String couple;
		for (i = 0; i < products.length; i++) {
			for (j = i + 1; j < products.length; j++) {
				/*
				 * Stabilisco un ordinamento tra le coppie, in modo che sia
				 * possibile confrontarle
				 */
				if (products[i].compareTo(products[j]) < 0) {
					couple = products[i] + "," + products[j];
				} else {
					couple = products[j] + "," + products[i];
				}
				couples.add(couple);
			}
		}
		return couples;
	}

}