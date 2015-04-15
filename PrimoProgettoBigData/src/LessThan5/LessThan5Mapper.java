package LessThan5;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (<prodotto1,prodotto2>,1) e le invia al
 * reducer per effettuare il conteggio
 *
 */
public class LessThan5Mapper extends Mapper<Object, Text, Text, IntWritable> {

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
			String[] productList = Arrays.copyOfRange(values, 1, values.length);

			Arrays.sort(productList);

			Set<String> partitions = new HashSet<String>();
			buildPartitions(partitions, productList, 0, "");

			/* Scrivo gli elementi */
			for (String partition : partitions) {
				context.write(new Text(partition), one);
			}
		}
	}

	private static void buildPartitions(Set<String> result,
			String[] productList, int arrayIndex, String actual) {

		/*
		 * Mi fermo se ho esaurito l'array o se sto costruendo stringhe troppo
		 * lunghe
		 */
		if (arrayIndex == productList.length || actual.split(",").length > 3) {
			return;
		}

		if (!actual.isEmpty()) {
			/* Aggiungo la parola corrente alla stringa che ho già formato */
			result.add(actual + "," + productList[arrayIndex]);
			buildPartitions(result, productList, arrayIndex + 1, actual + ","
					+ productList[arrayIndex]);
			buildPartitions(result, productList, arrayIndex + 1, actual);
		}

		buildPartitions(result, productList, arrayIndex + 1,
				productList[arrayIndex]);

	}

}