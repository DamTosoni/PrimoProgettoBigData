package SalesTrend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class TrimesterWritable implements Writable {

	private IntWritable firstMonth;
	private IntWritable secondMonth;
	private IntWritable thirdMonth;

	public TrimesterWritable() {
	}

	public TrimesterWritable(IntWritable firstMonth, IntWritable secondMonth,
			IntWritable thirdMonth) {
		this.firstMonth = firstMonth;
		this.secondMonth = secondMonth;
		this.thirdMonth = thirdMonth;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		String[] months = in.readUTF().split(" ");

		/* Adesso prendo i valori per ciascun trimestre */

		this.firstMonth = new IntWritable(
				Integer.parseInt(months[0].split(":")[1]));
		this.secondMonth = new IntWritable(Integer.parseInt(months[1]
				.split(":")[1]));
		this.thirdMonth = new IntWritable(
				Integer.parseInt(months[2].split(":")[1]));
	}

	@Override
	public void write(DataOutput out) throws IOException {

		String result = "1/2015:" + this.firstMonth + " 2/2015:"
				+ this.secondMonth + " 3/2015:" + this.thirdMonth;
		out.writeUTF(result);
	}

	@Override
	public String toString() {
		return "1/2015:" + this.firstMonth + " 2/2015:" + this.secondMonth
				+ " 3/2015:" + this.thirdMonth;
	}

	public static TrimesterWritable read(DataInput in) throws IOException {
		TrimesterWritable writable = new TrimesterWritable();
		writable.readFields(in);
		return writable;
	}

	public void set(IntWritable firstMonth, IntWritable secondMonth,
			IntWritable thirdMonth) {
		this.firstMonth = firstMonth;
		this.secondMonth = secondMonth;
		this.thirdMonth = thirdMonth;
	}

	public IntWritable getFirstMonth() {
		return firstMonth;
	}

	public IntWritable getSecondMonth() {
		return secondMonth;
	}

	public IntWritable getThirdMonth() {
		return thirdMonth;
	}

}