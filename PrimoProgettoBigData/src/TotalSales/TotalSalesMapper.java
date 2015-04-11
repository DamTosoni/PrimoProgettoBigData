package TotalSales;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalSalesMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {

	public static boolean isValidDate(String inDate) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		dateFormat.setLenient(false);
		try {
			dateFormat.parse(inDate.trim());
		} catch (ParseException pe) {
			return false;
		}
		return true;
	}

	public static List<List<String>> createLists(String input) {
		List<List<String>> result = new ArrayList<List<String>>();		
		StringTokenizer tokenizer = new StringTokenizer(input, ",");
		List<String> temp = null;
		String current = "";

		while (tokenizer.hasMoreTokens()) {
			current = tokenizer.nextToken();

			if (isValidDate(current)) {
				if (temp != null) {
					result.add(temp);
				}
				temp = new ArrayList<String>();
			} else {
				temp.add(current);
			}
		}
		
		if (temp != null) {
			result.add(temp);
		}

		return result;
	}

	private class Couple {
		public String item1, item2;

		public Couple(String item1, String item2) {
			this.item1 = item1;
			this.item2 = item2;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((item1 == null) ? 0 : item1.hashCode());
			result = prime * result + ((item2 == null) ? 0 : item2.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Couple other = (Couple) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (item1 == null) {
				if (other.item1 != null)
					return false;
			} else if (!item1.equals(other.item1))
				return false;
			if (item2 == null) {
				if (other.item2 != null)
					return false;
			} else if (!item2.equals(other.item2))
				return false;
			return true;
		}

		public String getItem1() {
			return item1;
		}

		public String getItem2() {
			return item2;
		}

		private TotalSalesMapper getOuterType() {
			return TotalSalesMapper.this;
		}
	};

	List<Couple> generated = new ArrayList<Couple>();

	private void generateCombinations(List<String> row) {
		Couple temp = null;

		for (String item1 : row) {
			for (String item2 : row) {
				if (!item1.equals(item2)) {
					temp = new Couple(item1, item2);

					if (!generated.contains(temp) && !generated.contains(new Couple(item2, item1))) {
						generated.add(temp);
					}
				}
			}
		}
	}

	private void tryToFit(Couple couple, List<List<String>> rows, Context context) throws IOException, InterruptedException {

		Text item = new Text();

		for (List<String> row : rows) {
			if (row.contains(couple.getItem1()) && row.contains(couple.getItem2())) {
				item.set(couple.getItem1() + "," + couple.getItem2());

				context.write(item, new IntWritable(1));
			}
		}
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String sales = value.toString();
		List<List<String>> rows = createLists(sales);

		for (List<String> row : rows) {
			generateCombinations(row);
		}

		for (Couple couple : generated) {
			tryToFit(couple, rows, context);
		}
	}
}