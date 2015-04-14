package CouplesFrequency;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ProductsListWritable implements Writable {

	private String[] productList;

	public ProductsListWritable() {
	}

	public ProductsListWritable(String[] productList) {
		this.productList = productList;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.productList = in.readUTF().split(",");
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.toString());
	}

	@Override
	public String toString() {
		String result = "";
		int i;

		for (i = 0; i < productList.length - 1; i++) {
			result += productList[i] + ",";
		}

		result += productList[i];
		return result;
	}

	public static ProductsListWritable read(DataInput in) throws IOException {
		ProductsListWritable writable = new ProductsListWritable();
		writable.readFields(in);
		return writable;
	}

	public String[] getProductList() {
		return productList;
	}
}