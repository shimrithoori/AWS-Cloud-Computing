package dataStructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class KeyPairData implements WritableComparable<KeyPairData> {
	private WordPair wordpair;
	private PairData pairdata;
	
	public KeyPairData(){
		wordpair = new WordPair();
		pairdata = new PairData();
	}
	
	public KeyPairData(WordPair owordpair,PairData opairdata){
		wordpair = new WordPair(owordpair.getw1().toString(), owordpair.getw2().toString(),owordpair.getdecade().get());
		pairdata = new PairData(opairdata.getcPair().get(), opairdata.getcw1().get(), opairdata.getcw2().get());
	}
	
	public void readFields(DataInput in) throws IOException {
		wordpair.readFields(in);
		pairdata.readFields(in);
		
		
	}

	public void write(DataOutput out) throws IOException {
		wordpair.write(out);
		pairdata.write(out);
	}

	public int compareTo(KeyPairData otherKeyPairData) {
		//int greaterThen = pairdata.getdecade().compareTo(otherKeyPairData.pairdata.getdecade()); 
		//if(greaterThen == 0)
			return wordpair.compareTo(otherKeyPairData.getwordPair());
		//if(greaterThen > 0)
		//	return 1;
		//return -1;
	}
	
//*************getters*******************//
	public PairData getpairData(){
		return this.pairdata;
	}
	public WordPair getwordPair(){
		return this.wordpair;
	}

//*************setters*******************//
/*		public void setpairData(pairData new_pairdata){
			//pairdata = new pairData(new_pairdata.getdecade(), new_pairdata.getcPair(), new_pairdata.get, ocw2, oN)
			pairdata = new_pairdata;
		}
		public void setwordPair(wordPair new_wordpair){
			wordpair = new_wordpair;
		}*/

	public String toString(){		
		return wordpair.toString() + "," + pairdata.toString() ;
	}
	
	

	
}
