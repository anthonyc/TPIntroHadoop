import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StringAndInt implements Comparable<StringAndInt>, Writable {
	final int LOWER = -1;
	final int EQUAL = 0;
	final int UPPER = 1;
	
	private String tag;
	private int numberOfOccurences;
	
	public StringAndInt() {
		
	}
	
	public StringAndInt(String tag, int numberOfOccurences) {
		this.tag = tag;
		this.numberOfOccurences = numberOfOccurences;
	}
	
	@Override
	public int compareTo(StringAndInt o) {
		if (this.numberOfOccurences < o.getNumberOfOccurences()) {
			return this.UPPER;
		}
		
		if (this.numberOfOccurences > o.getNumberOfOccurences()) {
			return this.LOWER;
		}
		
		return this.EQUAL;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.tag = in.readUTF();
		this.numberOfOccurences = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.tag);
		out.writeInt(this.numberOfOccurences);
	}
	
	public int getNumberOfOccurences() {
		return this.numberOfOccurences;
	}
	
	public String toString() {
		return this.tag;
	}
}
