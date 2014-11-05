package dataStructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class PMIwordPair implements WritableComparable<PMIwordPair> {
	
	public WordPair wordpair;
	public DoubleWritable PMI;
	
	public PMIwordPair() {
		wordpair = new WordPair();
		PMI = new DoubleWritable(0);
	}
	public PMIwordPair(double oPMI,WordPair owordpair){
		wordpair = new WordPair(owordpair.getw1().toString(), owordpair.getw2().toString(),owordpair.getdecade().get());
		PMI = new DoubleWritable(oPMI);
	}
	
	public PMIwordPair(PMIwordPair oPMIwordPair) {
		wordpair = new WordPair(oPMIwordPair.getwordpair().getw1().toString(), oPMIwordPair.getwordpair().getw2().toString(),oPMIwordPair.getwordpair().getdecade().get());
		PMI = new DoubleWritable(oPMIwordPair.getPMI().get());
	}
	

	public void readFields(DataInput in) throws IOException {
		wordpair.readFields(in);
		PMI.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		wordpair.write(out);
		PMI.write(out);
	}


	public int compareTo(PMIwordPair oPMIwordPair) {
		if((PMI.get() - oPMIwordPair.getPMI().get()) > 0) 
			return -1;
		else if ((PMI.get() - oPMIwordPair.getPMI().get()) < 0)
			return 1;
		else return wordpair.compareTo(oPMIwordPair.getwordpair());	
	}
	
	public String toString(){		
		return PMI + "," + wordpair.toString() ;
	}

	// *** Getters and Setters ***
	public DoubleWritable getPMI() {
		return PMI;
	}

	public WordPair getwordpair() {
		return wordpair;
	}

	public void setPMI(DoubleWritable pMI) {
		PMI = pMI;
	}

	public void setwordpair(WordPair wordpair) {
		this.wordpair = wordpair;
	}
	
}
