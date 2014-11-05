package calculateF;

import java.io.IOException;
import java.util.ArrayList;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;


public class calculateFmap {

	
	public static void main(String[] args) throws IOException {

		AWSCredentials credentials = new PropertiesCredentials(calculateFmap.class.getResourceAsStream("AwsCredentials.properties"));
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

		ArrayList<StepConfig> steps = new ArrayList<StepConfig>();

		HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig() 
		    .withJar("s3n://shimritadi/PartB/Step1.jar") 
		    .withArgs("s3n://shimritadi/PartA/output/D-Final/","s3n://shimritadi/PartB/Output/");
		  
		StepConfig stepConfig = new StepConfig() 
		    .withName("Step1") 
		    .withHadoopJarStep(hadoopJarStep) 
		    .withActionOnFailure("TERMINATE_JOB_FLOW"); 
		  
		steps.add(stepConfig);
		
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		    .withInstanceCount(2)
		    .withMasterInstanceType(InstanceType.M1Medium.toString())
		    .withSlaveInstanceType(InstanceType.M1Medium.toString())
		    .withHadoopVersion("2.4.0")
		    .withEc2KeyName("DSP-Task2")
		    .withKeepJobFlowAliveWhenNoSteps(false)
		    .withPlacement(new PlacementType("us-east-1b"));
	
	
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
			.withAmiVersion("3.1") 
			.withName("PartB")
			.withInstances(instances)
			.withSteps(steps)
			.withLogUri("s3n://shimritadi/PartB/Logs/");
		  
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest); 
		String jobFlowId = runJobFlowResult.getJobFlowId(); 
		System.out.println("Ran job flow with id: " + jobFlowId);
	}
	
}
