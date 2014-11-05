package dataStructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

public class PairDataN {
	
	private PairData pairdata;
	private LongWritable N;
	
	public PairDataN(){
		pairdata = new PairData();
		N = new LongWritable();
	}
	
	public PairDataN(PairData opairdata,long oN){
		pairdata = new PairData(opairdata);
		N = new LongWritable(oN);
	}
	
	public void readFields(DataInput in) throws IOException {
		pairdata.readFields(in);
		N.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		pairdata.write(out);
		N.write(out);
	}

	public int compareTo(PairDataN otherPairDataN) {
		if(pairdata.compareTo(otherPairDataN.getpairData()) > 0)
			return 1;
		return 0;
	}
	
//*************getters*******************//
	
	public PairData getpairData(){
		return this.pairdata;
	}
	public LongWritable getN(){
		return this.N;
	}
	
	public String toString(){		
		return pairdata.toString() + "," + N.get();
	}
	
}
