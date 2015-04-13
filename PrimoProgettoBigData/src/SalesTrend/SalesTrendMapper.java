package SalesTrend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (<prodotto1,prodotto2>,1) e le invia al
 * reducer per effettuare il conteggio
 *
 */
public class SalesTrendMapper extends
		Mapper<Object, Text, Text, TrimesterWritable> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] values = value.toString().split(",");
		/* Effettuo il parsing della data */
		String[] date = values[0].split("-");
		int month = Integer.parseInt(date[1]);

		if (date[0].equals("2015") && month < 4) {
			/*
			 * Lo scontrino e' relativo al primo trimestre del 2015. A questo
			 * punto per ogni prodotto scrivo la coppia (prodotto,
			 * <Mese>/2015:1)
			 */
			int i;
			int[] salesValues = new int[3];
			// Sfrutto il fatto che un array viene inizializzato con gli zeri
			salesValues[month - 1] = 1;

			for (i = 1; i < values.length; i++) {
				TrimesterWritable trimesterWritable = new TrimesterWritable(
						new IntWritable(salesValues[0]), new IntWritable(
								salesValues[1]),
						new IntWritable(salesValues[2]));
				context.write(new Text(values[i]), trimesterWritable);

			}
		}
	}
}