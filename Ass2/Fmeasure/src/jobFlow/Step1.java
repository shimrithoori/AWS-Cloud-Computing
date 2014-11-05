package jobFlow;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dataStructure.PMIwordPair;
import dataStructure.PairData;
import dataStructure.TestSet;
import dataStructure.WordPair;
import dataStructure.DecadePMI;

public class Step1 {
	
	public static class MapClass extends Mapper<LongWritable, Text, DecadePMI, PMIwordPair> {
		
		String[] parsed_keyvalue;
		String[] parsed_pairdata;
		WordPair wordpair;
		PairData pairdata;
		DecadePMI dpmi;
		PMIwordPair pmiwp;
		TestSet testset = new TestSet();
		
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	
	    	parsed_keyvalue = value.toString().split("\t");
	    	wordpair = new WordPair(parsed_keyvalue[0].toString().split(",")[0],
									parsed_keyvalue[0].toString().split(",")[1],
									Integer.parseInt(parsed_keyvalue[0].toString().split(",")[2]));
	    	parsed_pairdata = parsed_keyvalue[1].toString().split(",");
	    	pairdata = new PairData(Long.parseLong(parsed_pairdata[0]),
    										Long.parseLong(parsed_pairdata[1]),
    										Long.parseLong(parsed_pairdata[2]),
    										Long.parseLong(parsed_pairdata[3]));
	    	if(pairdata.getcw1().get()>0 && pairdata.getcw2().get()>0 && wordpair.getdecade().get()==10) {
	    		if(testset.contains(wordpair)) {
			    	double pmi = calcPMI(pairdata.getcPair().get(),pairdata.getcw1().get(),pairdata.getcw2().get(),pairdata.getN().get());
			    	dpmi = new DecadePMI(wordpair.getdecade().get(), new DoubleWritable(pmi));
			    	pmiwp = new PMIwordPair(pmi, wordpair);
			    	// send key: decadePMI - <decade, PMI>, value: PMIwordPair- <PMI, wordPair>
			    	System.out.println("step4: mapper: writing key "+ dpmi.toString()+" value- "+ pmiwp.toString());
			    	context.write(dpmi, pmiwp);
	    		}
	    	}
	    	
	    }
	}
	
	public static double calcPMI(long cPair,long cw1,long cw2, long N){
		return Math.log10(cPair) + Math.log10(N) - Math.log10(cw1) - Math.log10(cw2);
	}

	public static class PartitionerClass extends Partitioner<DecadePMI, PMIwordPair> {
		
		public int getPartition(DecadePMI key, PMIwordPair value, int numPartitions) {
			return 0;
	      
		}
	}
	

	public static class ReduceClass extends Reducer<DecadePMI,PMIwordPair,WordPair,DoubleWritable> {

		public void reduce(DecadePMI key, Iterable<PMIwordPair> values, Context context) throws IOException, InterruptedException {

			
			Iterator<PMIwordPair> it = values.iterator();
			while(it.hasNext()) {

				PMIwordPair pmiwp = new PMIwordPair(it.next());
				WordPair wp = new WordPair(pmiwp.getwordpair());
				DoubleWritable pmi = pmiwp.getPMI();
				
				context.write(wp, pmi);

			}
		}
	}
/*	
	public static class CompositeKeyComparator extends WritableComparator {
	    protected CompositeKeyComparator() {
	        super(DecadePMI.class, true);
	    }   
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	DecadePMI k1 = (DecadePMI)w1;
	    	DecadePMI k2 = (DecadePMI)w2;
	         
	    	double res = k1.getPMI().get() - k2.getPMI().get();
	    	if(res > 0)
	    		return -1;
	    	if(res < 0)
	    		return 1;
	    	return 0;

	    }
	}*/

	public static void main(String[] args) throws Exception { 
	    Configuration conf = new Configuration(); 
	    //conf.set("mapred.map.tasks","10"); 
	    //conf.set("mapred.reduce.tasks","2"); 
	    @SuppressWarnings("deprecation")
		Job job = new Job(conf, "calculateF"); 
	    job.setJarByClass(Step1.class); 
	    job.setMapperClass(MapClass.class); 
	    job.setNumReduceTasks(1);
	    job.setPartitionerClass(PartitionerClass.class); 
	    job.setReducerClass(ReduceClass.class); 
	   
	    FileInputFormat.addInputPath(job, new Path(args[0])); 
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
		job.setMapOutputKeyClass(DecadePMI.class);
		job.setMapOutputValueClass(PMIwordPair.class);
	
	    job.setOutputKeyClass(WordPair.class); 						//moved
	    job.setOutputValueClass(DoubleWritable.class);				//moved
	    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1); 
	        
	  } 
}