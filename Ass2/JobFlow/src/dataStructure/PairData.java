package dataStructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class PairData implements WritableComparable<PairData> {
	private LongWritable cPair;
	private LongWritable cw1;
	private LongWritable cw2;
	
	
	public PairData(){
		cPair = new LongWritable();
		cw1 = new LongWritable();
		cw2 = new LongWritable();
		//N = new LongWritable();
	}
	
	public PairData(PairData opairdata){
		cPair = new LongWritable(opairdata.getcPair().get());
		cw1 = new LongWritable(opairdata.getcw1().get());
		cw2 = new LongWritable(opairdata.cw2.get());
		//N = new LongWritable(opairdata.getN().get());
	}
	
	public PairData(long ocPair , long ocw1, long ocw2){
		cPair = new LongWritable(ocPair);
		cw1 = new LongWritable(ocw1);
		cw2 = new LongWritable(ocw2);
		//N = new LongWritable(oN);
	}
	public void readFields(DataInput in) throws IOException {
		cPair.readFields(in);
		cw1.readFields(in);
		cw2.readFields(in);
		//N.readFields(in);
		
	}

	public void write(DataOutput out) throws IOException {
		cPair.write(out);
		cw1.write(out);
		cw2.write(out);
		//N.write(out);
	}

	public int compareTo(PairData otherPairData) {
		if(cPair.compareTo(otherPairData.cPair) > 0)
			return 1;
		return 0;
	}
	
//*************getters*******************//
	
	public LongWritable getcPair(){
		return this.cPair;
	}
	public LongWritable getcw1(){
		return this.cw1;
	}
	public LongWritable getcw2(){
		return this.cw2;
	}
/*	public LongWritable getN(){
		return this.N;
	}
	*/
	
//*************setters*******************//
	public void setcPair(long new_cPair){
		cPair = new LongWritable(new_cPair);
	}
	public void setcw1(long new_cw1){
		cw1 = new LongWritable(new_cw1);
	}
	public void setcw2(long new_cw2){
		cw2 = new LongWritable(new_cw2);
	}
/*	public void setN(long new_N){
		N = new LongWritable(new_N);
	}
*/	
	public String toString(){		
		return cPair + "," + cw1 + "," + cw2;
	}
	
}
