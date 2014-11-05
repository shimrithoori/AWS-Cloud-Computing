package dataStructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class CompositeWordPair implements WritableComparable<CompositeWordPair> {
	private WordPair wordpair;
	private IntWritable num;
	
	public CompositeWordPair() {	
		wordpair = new WordPair();
		num = new IntWritable();
	}
	
	public CompositeWordPair(int onum, WordPair owordpair){
		wordpair = new WordPair(owordpair.getw1().toString(), owordpair.getw2().toString(),owordpair.getdecade().get());
		num = new IntWritable(onum);
	}
	
	public void readFields(DataInput in) throws IOException {
		wordpair.readFields(in);
		num.readFields(in);	
	}

	public void write(DataOutput out) throws IOException {
		wordpair.write(out);
		num.write(out);
		
	}

	public int compareTo(CompositeWordPair othercompositeWordpair) {
		int greaterThen = this.wordpair.compareTo(othercompositeWordpair.getwordpair()); 
		if(greaterThen == 0)//if it is the same pair (w1,*)
			return this.num.get() - othercompositeWordpair.getnum().get();
		return greaterThen;
	}

//*************getters*******************//
	public IntWritable getnum(){
			return this.num;
	}
	
	public WordPair getwordpair(){
		return this.wordpair;
	}	
	
	public String toString(){
		return num +","+ wordpair.toString();
	}

}


