package jobFlow;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import dataStructure.PMIwordPair;
import dataStructure.PairData;
import dataStructure.PairDataN;
import dataStructure.WordPair;
import dataStructure.DecadePMI;;

public class Step5 {
	
	public static class MapClass extends Mapper<LongWritable, Text, DecadePMI, PMIwordPair> {
		
		DecadePMI dpmi;
		PMIwordPair pmiwp;
		
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	
	    	String[] parsed_keyvalue = value.toString().split("\t");
	    	WordPair wordpair = new WordPair(parsed_keyvalue[0].toString().split(",")[0],
									parsed_keyvalue[0].toString().split(",")[1],
									Integer.parseInt(parsed_keyvalue[0].toString().split(",")[2]));
	    	String[] parsed_pairdata = parsed_keyvalue[1].toString().split(",");
	    	PairDataN pairdatan = new PairDataN(new PairData(Long.parseLong(parsed_pairdata[0]),
    										Long.parseLong(parsed_pairdata[1]),
    										Long.parseLong(parsed_pairdata[2])),
    										Long.parseLong(parsed_pairdata[3]));
	    	if(pairdatan.getpairData().getcw1().get()>0 && pairdatan.getpairData().getcw2().get()>0) {	
		    	double pmi = calcPMI(pairdatan.getpairData().getcPair().get(),
		    						pairdatan.getpairData().getcw1().get(),
		    						pairdatan.getpairData().getcw2().get(),
		    						pairdatan.getN().get());
		    	
		    	dpmi = new DecadePMI(wordpair.getdecade().get(), new DoubleWritable(pmi));
		    	pmiwp = new PMIwordPair(pmi, wordpair);
		    	// send key: decadePMI - <decade, PMI>, value: PMIwordPair- <PMI, wordPair>
		    	context.write(dpmi, pmiwp);
	    	}
	    }
	}
	
	public static class PartitionerClass extends Partitioner<DecadePMI, PMIwordPair> {
		
		public int getPartition(DecadePMI key, PMIwordPair value, int numPartitions) {
			return key.getdecade().get();
	      }
	    }
	

	public static class ReduceClass extends Reducer<DecadePMI,PMIwordPair,WordPair,DoubleWritable> {
		int k ;
		int i = 0;
		
		public void reduce(DecadePMI key, Iterable<PMIwordPair> values, Context context) throws IOException, InterruptedException {
//			k = Integer.parseInt(context.getConfiguration().get("k","-1"));
			k = 40;
			
			Iterator<PMIwordPair> it = values.iterator();
//			while(i < k && values.iterator().hasNext()) {
			while(i < k && it.hasNext()) {

				PMIwordPair pmiwp = new PMIwordPair(it.next());
				WordPair wp = new WordPair(pmiwp.getwordpair());
				DoubleWritable pmi = pmiwp.getPMI();
				
				context.write(wp, pmi);
				i++;
			}
		}
	}
	
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
	}

	
	public static double calcPMI(long cPair,long cw1,long cw2, long N){
		return Math.log(cPair) + Math.log(N) - Math.log(cw1) - Math.log(cw2);
	}
		
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
			System.out.println("jobflow");
//			conf.set("k", args[2]); 
		    @SuppressWarnings("deprecation")
			Job job = new Job(conf, "step5");
		    
		    job.setJarByClass(Step5.class);
		    job.setMapperClass(MapClass.class);
		    job.setPartitionerClass(PartitionerClass.class);
		    job.setReducerClass(ReduceClass.class);

	        job.setSortComparatorClass(CompositeKeyComparator.class); 
		    
		    job.setNumReduceTasks(12);
			System.out.println("try to locate bucket");
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    System.out.println("try to locate output location");
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setMapOutputKeyClass(DecadePMI.class);
			job.setMapOutputValueClass(PMIwordPair.class);
		    job.setOutputKeyClass(WordPair.class);
			job.setOutputValueClass(DoubleWritable.class);
			
	 	    boolean completed = job.waitForCompletion(true);
	 	    if(completed) {
	 	        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(Step5.class.getResourceAsStream("AwsCredentials.properties")));
	 	        String jobFlowSqsUrl= sqs.getQueueUrl("job_flow1_sqs").getQueueUrl(); 

	 	        long step5count = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
	 	        List<Message> messagesList = sqs.receiveMessage(new ReceiveMessageRequest(jobFlowSqsUrl)).getMessages();
	 	        Message message = messagesList.get(0);
				String messageRecieptHandle = message.getReceiptHandle();
	            sqs.deleteMessage(new DeleteMessageRequest(jobFlowSqsUrl, messageRecieptHandle));
	            
	 	        long step4count = Long.parseLong(message.getBody());
	 	        long new_step5count = step4count + step5count;
	 	        sqs.sendMessage(new SendMessageRequest(jobFlowSqsUrl, new_step5count+""));
	 	        
	 	        System.out.println("*** Num of Key Values: " + new_step5count + " ***");
	 	    }
	
	         System.exit(completed ? 0 : 1);
	 }
}
