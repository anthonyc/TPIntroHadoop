import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StringAndIntWritable implements WritableComparable<StringAndInt> {
	final int LOWER = -1;
	final int EQUAL = 0;
	final int UPPER = 1;
	
	private Text tag;
	private IntWritable numberOfOccurences;
	
	public StringAndIntWritable() {
		this.tag = new Text("");
		this.numberOfOccurences = new IntWritable(0);
	}
	
	public StringAndIntWritable(Text tag, IntWritable numberOfOccurences) {
		this.tag = tag;
		this.numberOfOccurences = numberOfOccurences;
	}
	
	@Override
	public int compareTo(StringAndInt o) {
		if (this.numberOfOccurences.get() < o.getNumberOfOccurences()) {
			return this.UPPER;
		}
		
		if (this.numberOfOccurences.get() > o.getNumberOfOccurences()) {
			return this.LOWER;
		}
		
		return this.EQUAL;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.tag.readFields(in);
		this.numberOfOccurences.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.tag.write(out);
		this.numberOfOccurences.write(out);
	}
	
	public int getNumberOfOccurences() {
		return this.numberOfOccurences.get();
	}
	
	public String toString() {
		return this.tag.toString();
	}
}
