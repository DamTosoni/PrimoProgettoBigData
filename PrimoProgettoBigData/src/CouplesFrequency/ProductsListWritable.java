package CouplesFrequency;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ProductsListWritable implements Writable {

	private CoupleProductOccurrence[] productList;

	public ProductsListWritable() {
	}

	public ProductsListWritable(CoupleProductOccurrence[] productList) {
		this.productList = productList;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String[] coupleDescriptions = in.readUTF().split(",");
		int length = coupleDescriptions.length;
		this.productList = new CoupleProductOccurrence[length];
		int i;
		for (i = 0; i < length; i++) {
			this.productList[i] = new CoupleProductOccurrence(
					coupleDescriptions[i]);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.toString());
	}

	@Override
	public String toString() {

		if (productList.length == 0) {
			return "";
		}

		String result = "";
		int i;

		String coupleString;
		for (i = 0; i < productList.length - 1; i++) {
			coupleString = productList[i].toString();
			result += coupleString + ",";
		}

		result += productList[i];
		return result;

	}

	public static ProductsListWritable read(DataInput in) throws IOException {
		ProductsListWritable writable = new ProductsListWritable();
		writable.readFields(in);
		return writable;
	}

	public CoupleProductOccurrence[] getProductList() {
		return productList;
	}
}