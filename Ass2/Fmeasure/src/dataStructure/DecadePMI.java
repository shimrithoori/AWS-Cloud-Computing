package dataStructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class DecadePMI implements WritableComparable<DecadePMI> {
	
	IntWritable decade;
	DoubleWritable PMI;
	
	public DecadePMI(){
		decade= new IntWritable(0);
		PMI = new DoubleWritable(0);
	}
	
	public DecadePMI(int odecade, DoubleWritable oPMI) {
		decade = new IntWritable(odecade);
		PMI = new DoubleWritable(oPMI.get());
	}
	
	public void readFields(DataInput in) throws IOException {
		decade.readFields(in);
		PMI.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		decade.write(out);
		PMI.write(out);
	}
	
	public int compareTo(DecadePMI odwp) {
		if(this.decade.equals(odwp.getdecade())){
			double res = this.PMI.get() - odwp.getPMI().get();
			if(res > 0)
				return -1;
			if(res < 0)
				return 1;
			return 0;
		}
		return this.decade.get() - odwp.getdecade().get();
	}
	
	public String toString() {
		return decade + "," + PMI.toString();
	}
	// *** Getters and Setters ***
	public IntWritable getdecade() {
		return decade;
	}

	public DoubleWritable getPMI() {
		return PMI;
	}
}


