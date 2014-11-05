package jobFlow;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import dataStructure.PairData;
import dataStructure.StopWords;
import dataStructure.WordPair;

public class Step1 {
	
	public static class MapClass extends Mapper<LongWritable, Text, WordPair, PairData> {
		
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
	    	StopWords stopWords = new StopWords();
	    	int year = -1;
	    	long occurrences = -1;
	    	
			StringTokenizer line = new StringTokenizer(value.toString());
			int numOfWords = line.countTokens()-4;
			
			if(numOfWords>1) {
				String[] nGramData = getNGramData(line,numOfWords); 
				String[] nGram = Arrays.copyOfRange(nGramData,0,numOfWords); 
				year = Integer.parseInt(nGramData[numOfWords]);
				occurrences = Long.parseLong(nGramData[numOfWords+1]);
				
				if(occurrences == -1 || year == -1) {
					System.out.println("Step1: Curropted ngram");
					return; //?
				}
			
				String middleWord = unPunc(nGram[(numOfWords-1)/2]);
			    long numOfRealWords = 0;
			    	
		    	if(! (nGram.length<=1 || year < 1900 || stopWords.contains(middleWord) || middleWord == "")) {
		    		for(String word : nGram) {
		    			word = unPunc(word);
		    			if(!(word == "" || word.equals(middleWord) || stopWords.contains(word))) {
		    				numOfRealWords++;
		    				WordPair new_wordPair = new WordPair(word, middleWord,(year - 1900)/10);
		    				context.write(new_wordPair,new PairData(occurrences,0,0));//sends (w1,w2)
		    			}
		    		}

		    		//sends (*,*) with N
		    		if(numOfRealWords>0) {
		    			context.write(new WordPair("*", "*",(year - 1900)/10),new PairData((numOfRealWords+1) * occurrences,0,0));
		    		}
		    		//sends (middleword,*) with the number of repeated occurrences(if bigger then 0) 
		    		if((occurrences * -1 * (numOfRealWords-1)) < 0) {
		    			context.write(new WordPair(middleWord, "*",(year - 1900)/10), new PairData(0, 0, occurrences * -1 * (numOfRealWords-1)));
		    		}
		    	}
			}
	    }

	}


	public static class PartitionerClass extends Partitioner<WordPair, PairData> {
		
		public int getPartition(WordPair key, PairData value, int numPartitions) {
			return key.getdecade().get();
	      }
	}
	
	
	public static class ReduceClass extends Reducer<WordPair,PairData,WordPair,PairData> {
		
		LongWritable N = new LongWritable(0);
			
		public void reduce(WordPair key, Iterable<PairData> values, Context context) throws IOException,  InterruptedException {
			
			if(key.getw1().toString().equals("*") && key.getw2().toString().equals("*")){// if (*,*)
					for(PairData pairdata : values) //write to global N
						N.set(N.get() + pairdata.getcPair().get());
					context.write(key, new PairData(N.get(), 0, 0)); //writes the sum of N for that decade
			}
			else{	
				long cw2 = 0;
				long cPair = 0;	
				if(key.getw2().toString().equals("*")) //if (w1,*)
					for(PairData pairdata : values)
						cw2 += pairdata.getcw2().get(); 
				else//(w1,w2)
					for(PairData pairdata : values)
						cPair += pairdata.getcPair().get();
				context.write(key, new PairData(cPair, 0, cw2));
			}
		}
	}
	
	
	private static String unPunc(String str){
		String unPuncedWord="";
		String punctuations = ",.:;{}()[]/1234567890~`!@#$%^&*_—-+=°•'<>£§«¿€|©®ЫЛвим�™×׀,׀ ֳ‰µƒ��" + '"' + "\\";
		boolean punc = false;
		
		for(int i=0; i<str.length(); i++){
			puncLoop:
			for(int j=0; j<punctuations.length(); j++){
				if(str.charAt(i)==punctuations.charAt(j)){
					punc = true;
					break puncLoop;
				}
			}
			if(!punc)
				unPuncedWord= unPuncedWord+ str.charAt(i);
			punc = false;
		}
		return unPuncedWord;
	}
	
	private static String[] getNGramData(StringTokenizer line,int numOfWords) {		
		String[] nGramData = new String[numOfWords+2];
		int i = 0;
		for(i = 0; line.hasMoreElements() && i<numOfWords; i++){
			nGramData[i] = line.nextElement().toString();
		}
		nGramData[i] = line.nextElement().toString();
		nGramData[++i] = line.nextElement().toString();
			
		return nGramData;
	}
	
	
	 public static void main(String[] args) throws Exception {
         Configuration conf = new Configuration();
         @SuppressWarnings("deprecation")
         Job job = new Job(conf, "jobFlow");
         
         job.setJarByClass(Step1.class);
         job.setMapperClass(MapClass.class);
         job.setPartitionerClass(PartitionerClass.class);
         job.setCombinerClass(ReduceClass.class);
         job.setReducerClass(ReduceClass.class);
         
         job.setInputFormatClass(SequenceFileInputFormat.class);

         job.setNumReduceTasks(12);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));

         job.setMapOutputKeyClass(WordPair.class);
         job.setMapOutputValueClass(PairData.class);
         
         job.setOutputKeyClass(WordPair.class); 					
 	     job.setOutputValueClass(PairData.class);		
 	     
 	    boolean completed = job.waitForCompletion(true);
	    if(completed) {
 	    	 // Sending count to SQS
 	 	     AWSCredentials credentials = new PropertiesCredentials(Step1.class.getResourceAsStream("AwsCredentials.properties"));
 	 	     AmazonSQS sqs = new AmazonSQSClient(credentials);
 	         String jobFlowSqsUrl= sqs.createQueue(new CreateQueueRequest("job_flow1_sqs")).getQueueUrl() ;
 	         long step1count = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
 	         sqs.sendMessage(new SendMessageRequest(jobFlowSqsUrl, step1count+""));
 	    }
 	     
         System.exit(completed ? 0 : 1);
         
 	}
}

	