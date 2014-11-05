package jobFlow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dataStructure.WordPair;
import dataStructure.TestSet;

public class step2 {

	public static class MapClass extends Mapper<LongWritable, Text, WordPair, DoubleWritable> {
		
		TestSet testset = new TestSet();
		public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			
			String[] parsedKeyVal = value.toString().split("\t");
			WordPair wordpair = new WordPair(parsedKeyVal[0].split(" ")[0].toString(), parsedKeyVal[0].split(" ")[1].toString(),10);
			double pmi = Double.parseDouble(parsedKeyVal[1]);
			
			if(testset.contains(wordpair)) {
				context.write(wordpair, new DoubleWritable(pmi));
			}
		}
	}
	
	public static void main(String[] args) throws Exception { 
	    Configuration conf = new Configuration(); 
	    //conf.set("mapred.map.tasks","10"); 
	    //conf.set("mapred.reduce.tasks","2"); 
	    @SuppressWarnings("deprecation")
		Job job = new Job(conf, "calculateF"); 
	    job.setJarByClass(step2.class); 
	    job.setMapperClass(MapClass.class); 
	    job.setNumReduceTasks(0);
//	    job.setPartitionerClass(PartitionerClass.class); 
//	    job.setCombinerClass(ReduceClass.class); 
//	    job.setReducerClass(ReduceClass.class); 
	    
	    job.setInputFormatClass(SequenceFileInputFormat.class); //added here
	    
	    FileInputFormat.addInputPath(job, new Path(args[1])); 
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
	    
	    job.setOutputKeyClass(WordPair.class); 						//moved
	    job.setOutputValueClass(DoubleWritable.class);				//moved
	    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1); 
	        
	  } 
}

