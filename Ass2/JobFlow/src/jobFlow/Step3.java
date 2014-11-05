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

import dataStructure.CompositeWordPair;
import dataStructure.KeyPairData;
import dataStructure.PairData;
import dataStructure.WordPair;

public class Step3 {
	
	public static class MapClass extends Mapper<LongWritable, Text, CompositeWordPair, KeyPairData> {
	
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {

	    	String[] parsed_keyvalue = value.toString().split("\t");
	    	WordPair wordpair = new WordPair(parsed_keyvalue[0].toString().split(",")[0],
	    									parsed_keyvalue[0].toString().split(",")[1],
	    									Integer.parseInt(parsed_keyvalue[0].toString().split(",")[2]));
	    	String[] parsed_keypairdata = parsed_keyvalue[1].toString().split(",");
	    	KeyPairData keypairdata = new KeyPairData(new WordPair(parsed_keypairdata[0],
	    												parsed_keypairdata[1],
	    												Integer.parseInt(parsed_keypairdata[2])),
	    											new PairData(Long.parseLong(parsed_keypairdata[3]),
					    										Long.parseLong(parsed_keypairdata[4]),
					    										Long.parseLong(parsed_keypairdata[5])));
	    	
	    	if(keypairdata.getwordPair().getw1().toString().equals("*"))//if(*,*)
	    		context.write(new CompositeWordPair(0, wordpair), keypairdata);
	    	else{//(w1,*)/(w1,w2)
	    		if(!(keypairdata.getwordPair().getw2().toString().equals("*")))//if(w1,w2)
	    			context.write(new CompositeWordPair(1, wordpair), keypairdata);
	    	}		
	    }
	}
	
	
	public static class PartitionerClass extends Partitioner<CompositeWordPair, KeyPairData> {
		
		public int getPartition(CompositeWordPair key, KeyPairData value, int numPartitions) {
			return key.getwordpair().getdecade().get();
	      }
	    }
	
	
	
	public static class ReduceClass extends Reducer<CompositeWordPair,KeyPairData,WordPair,PairData> {
		LongWritable cw = new LongWritable(0);

		public void reduce(CompositeWordPair key, Iterable<KeyPairData> values, Context context) throws IOException,  InterruptedException {
			if(key.getwordpair().getw1().toString().equals("*"))//(*,*)
				context.write(key.getwordpair(), values.iterator().next().getpairData());
			
			else{
				for(KeyPairData keypairdata : values){
					if(keypairdata.getwordPair().getw1().toString().equals("*") && keypairdata.getwordPair().getw2().toString().equals("*"))
						cw.set(keypairdata.getpairData().getcPair().get());
					else{
						if(key.getwordpair().getw1().toString().equals(keypairdata.getwordPair().getw1().toString()))
							keypairdata.getpairData().setcw1(cw.get());
						
						else
							keypairdata.getpairData().setcw2(cw.get());
						context.write(keypairdata.getwordPair(),keypairdata.getpairData());
					}
				}
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "jobFlow");
        
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setNumReduceTasks(12);
        FileInputFormat.addInputPath(job, new Path(args[0]));//0?
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//1?
        job.setMapOutputKeyClass(CompositeWordPair.class);
        job.setMapOutputValueClass(KeyPairData.class);
        
        job.setOutputKeyClass(WordPair.class); 					
	    job.setOutputValueClass(PairData.class);			
        
 	    boolean completed = job.waitForCompletion(true);
 	    if(completed) {
 	        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(Step3.class.getResourceAsStream("AwsCredentials.properties")));
 	        String jobFlowSqsUrl= sqs.getQueueUrl("job_flow1_sqs").getQueueUrl(); 

 	        long step3count = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
 	        List<Message> messagesList = sqs.receiveMessage(new ReceiveMessageRequest(jobFlowSqsUrl)).getMessages();
 	        Message message = messagesList.get(0);
			String messageRecieptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(jobFlowSqsUrl, messageRecieptHandle));
 	        long step2count = Long.parseLong(message.getBody());
 	        long new_step3count = step2count + step3count;
 	        sqs.sendMessage(new SendMessageRequest(jobFlowSqsUrl, new_step3count+""));
 	    }
 	     
         System.exit(completed ? 0 : 1);
	 }
}
