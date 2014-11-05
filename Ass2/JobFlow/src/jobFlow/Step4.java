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

import dataStructure.PairData;
import dataStructure.PairDataN;
import dataStructure.WordPair;

public class Step4 {

	public static class MapClass extends Mapper<LongWritable, Text, WordPair, PairData> {
		
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	
	    	String[] parsed_keyvalue = value.toString().split("\t");
	    	WordPair wordpair = new WordPair(parsed_keyvalue[0].toString().split(",")[0],
	    									parsed_keyvalue[0].toString().split(",")[1],
	    									Integer.parseInt(parsed_keyvalue[0].toString().split(",")[2]));
	    	String[] parsed_pairdata = parsed_keyvalue[1].toString().split(",");
	    	PairData pairdata = new PairData(Long.parseLong(parsed_pairdata[0]),
	    										Long.parseLong(parsed_pairdata[1]),
	    										Long.parseLong(parsed_pairdata[2]));
	    	context.write(wordpair, pairdata);
	    	
	    }
	}
	    
	
	public static class CombinarClass extends Reducer<WordPair,PairData,WordPair,PairData> {
		
		public void reduce(WordPair key,Iterable<PairData> values, Context context) throws IOException, InterruptedException {
			long cw1 = 0;
			long cw2 = 0;
			long cPair = 0;
			
			if(key.getw1().toString().equals("*"))
				context.write(key, values.iterator().next());
			else{ //each wordPair has a list of 2 pairData
				for(PairData pairdata : values) {
					cPair = pairdata.getcPair().get();
					cw1 += pairdata.getcw1().get();
					cw2 += pairdata.getcw2().get();
				}
				context.write(key,new PairData(cPair, cw1, cw2));
			}
		}
	}	

	public static class PartitionerClass extends Partitioner<WordPair, PairData> {
		
		public int getPartition(WordPair key, PairData value, int numPartitions) {
			return key.getdecade().get();
	      }
	 }
	
	public static class ReduceClass extends Reducer<WordPair,PairData,WordPair,PairDataN> {
		
		LongWritable N = new LongWritable(0);

		public void reduce(WordPair key, Iterable<PairData> values, Context context) throws IOException,  InterruptedException {
			long cw1 = 0;
			long cw2 = 0;
			long cPair = 0;
			
			if(key.getw1().toString().equals("*"))
				N.set(values.iterator().next().getcPair().get());
			
			else{ //each wordPair has a list of 2 pairData
				for(PairData pairdata : values) {
					cPair = pairdata.getcPair().get();
					cw1 += pairdata.getcw1().get();
					cw2 += pairdata.getcw2().get();
				}
				context.write(key, new PairDataN(new PairData(cPair, cw1, cw2),N.get()));
			}
		}
	}
	
		
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
			System.out.println("jobflow");
		    @SuppressWarnings("deprecation")
			Job job = new Job(conf, "step4");
		    
		    job.setJarByClass(Step4.class);
		    
		    job.setMapperClass(MapClass.class);
		    job.setPartitionerClass(PartitionerClass.class);
		    job.setCombinerClass(CombinarClass.class);
		    job.setReducerClass(ReduceClass.class);
		   
		    job.setNumReduceTasks(12);
			System.out.println("try to locate bucket");
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    System.out.println("try to locate output location");
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setMapOutputKeyClass(WordPair.class);
			job.setMapOutputValueClass(PairData.class);
		    job.setOutputKeyClass(WordPair.class);
			job.setOutputValueClass(PairDataN.class);
			
	 	    boolean completed = job.waitForCompletion(true);
	 	    if(completed) {
	 	        AmazonSQS sqs = new AmazonSQSClient(new PropertiesCredentials(Step4.class.getResourceAsStream("AwsCredentials.properties")));
	 	        String jobFlowSqsUrl= sqs.getQueueUrl("job_flow1_sqs").getQueueUrl(); 

	 	        long step4count = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
	 	        List<Message> messagesList = sqs.receiveMessage(new ReceiveMessageRequest(jobFlowSqsUrl)).getMessages();
	 	        Message message = messagesList.get(0);
				String messageRecieptHandle = message.getReceiptHandle();
	            sqs.deleteMessage(new DeleteMessageRequest(jobFlowSqsUrl, messageRecieptHandle));
	 	        long step3count = Long.parseLong(message.getBody());
	 	        long new_step4count = step3count + step4count;
	 	        sqs.sendMessage(new SendMessageRequest(jobFlowSqsUrl, new_step4count+""));
	 	    }
	         System.exit(completed ? 0 : 1);
	 }
	
}
