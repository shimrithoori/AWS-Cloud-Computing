package jobFlow;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

import dataStructure.KeyPairData;
import dataStructure.PairData;
import dataStructure.WordPair;

public class Step2 {
	
	public static class MapClass extends Mapper<LongWritable, Text, WordPair, KeyPairData> {
		
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	String[] parsed_keyvalue = value.toString().split("\t");
	    	WordPair wordpair = new WordPair(parsed_keyvalue[0].toString().split(",")[0],
	    									parsed_keyvalue[0].toString().split(",")[1],
	    									Integer.parseInt(parsed_keyvalue[0].toString().split(",")[2]));
	    	
	    	String[] parsed_pairdata = parsed_keyvalue[1].toString().split(",");
    		PairData pairdata = new PairData(Long.parseLong(parsed_pairdata[0]),
    										Long.parseLong(parsed_pairdata[1]),
    										Long.parseLong(parsed_pairdata[2]));
    		
	    	if(wordpair.getw2().toString().equals("*"))//if (w1,*)/(*,*)
				context.write(wordpair, new KeyPairData(wordpair,pairdata));	
	    	
	    	else{//(w1,w2)
    			context.write(new WordPair(wordpair.getw1().toString(), "*",wordpair.getdecade().get()),
    						new KeyPairData(wordpair, new PairData(pairdata.getcPair().get(),
																	pairdata.getcPair().get(),
																	pairdata.getcw2().get())));
    			
    			context.write(new WordPair(wordpair.getw2().toString(), "*",wordpair.getdecade().get()),
    						new KeyPairData(wordpair, new PairData(pairdata.getcPair().get(),
																	pairdata.getcw1().get(),
																	pairdata.getcPair().get()))); 
	    	}
	    }
	}
	
	
	public static class PartitionerClass extends Partitioner<WordPair, KeyPairData> {
		
		public int getPartition(WordPair key, KeyPairData value, int numPartitions) {
			return key.getdecade().get();
	      }
	}
	
	
	public static class ReduceClass extends Reducer<WordPair,KeyPairData,WordPair,KeyPairData> {

		public void reduce(WordPair key, Iterable<KeyPairData> values, Context context) throws IOException,  InterruptedException {
			long cw = 0;
			if(key.getw1().toString().equals("*"))//if(*,*)
				context.write(key, values.iterator().next());
			else{
				for(KeyPairData keypairdata : values){
					cw += keypairdata.getpairData().getcw1().get() + keypairdata.getpairData().getcw2().get();
					context.write(key, keypairdata);
				}
				context.write(key, new KeyPairData(new WordPair("*", "*",key.getdecade().get()),new PairData(cw, 0 , 0)));
			}
		}
	}
	
		
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "jobFlow");
        
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReduceClass.class);
        job.setReducerClass(ReduceClass.class);
        

        job.setNumReduceTasks(12);
        FileInputFormat.addInputPath(job, new Path(args[0]));//0?
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//1?

        job.setMapOutputKeyClass(WordPair.class);
        job.setMapOutputValueClass(KeyPairData.class);
        
        job.setOutputKeyClass(WordPair.class); 					
	    job.setOutputValueClass(KeyPairData.class);		
	    
 	    boolean completed = job.waitForCompletion(true);
 	    if(completed) {
 	        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(Step2.class.getResourceAsStream("AwsCredentials.properties")));
 	        String jobFlowSqsUrl= sqs.getQueueUrl("job_flow1_sqs").getQueueUrl(); 

 	        long step2count = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
 	        List<Message> messagesList = sqs.receiveMessage(new ReceiveMessageRequest(jobFlowSqsUrl)).getMessages();
 	        Message message = messagesList.get(0);
			String messageRecieptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(jobFlowSqsUrl, messageRecieptHandle));
 	        long step1count = Long.parseLong(message.getBody());
 	        long new_step2count = step2count + step1count;
 	        sqs.sendMessage(new SendMessageRequest(jobFlowSqsUrl, new_step2count+""));
 	    }
 	     
         System.exit(completed ? 0 : 1);
	 }
}
