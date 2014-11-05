package dataStructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable<WordPair>{
	private Text w1;
	private Text w2;
	private IntWritable decade;

	public WordPair(){
		w1 = new Text();
		w2 = new Text();
		decade = new IntWritable();
	}
	
	public WordPair(String ow1, String ow2,int odecade){
		int greaterThen = ow1.compareTo(ow2);		
		if(greaterThen == 0 || ( greaterThen < 0 && !ow1.equals("*")) || ( greaterThen > 0 && ow2.equals("*"))){
			w1 = new Text(ow1);
			w2 = new Text(ow2);
		}
		else{
			w1 = new Text(ow2);
			w2 = new Text(ow1);
		}
		decade = new IntWritable(odecade);	
	
	}
	public WordPair(WordPair otherWordPair) {
		w1 = new Text(otherWordPair.getw1().toString());
		w2 = new Text(otherWordPair.getw2().toString());
		decade = new IntWritable(otherWordPair.getdecade().get());
	}

	public void readFields(DataInput in) throws IOException {
		w1.readFields(in);
		w2.readFields(in);
		decade.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		w1.write(out);
		w2.write(out);
		decade.write(out);
	}

	public int compareTo(WordPair otherWordpair) {
		
		if(decade.equals(otherWordpair.getdecade())){
			int greaterThen =  w1.toString().compareTo(otherWordpair.getw1().toString());
			if(greaterThen != 0)
				return greaterThen;
			return w2.toString().compareTo(otherWordpair.getw2().toString());
		}
		return decade.compareTo(otherWordpair.getdecade());
	}

//*************getters*******************//
	public Text getw1(){
			return this.w1;
	}
	
	public Text getw2(){
		return this.w2;
	}	
	
	public IntWritable getdecade(){
		return this.decade;
	}
	
	public String toString(){
		return w1.toString() +","+ w2.toString() + "," + decade;
	}	
}
