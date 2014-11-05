package conf;

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


public class ExtractRelatedPairs {

	public static void main(String[] args) throws IOException {
		
		AWSCredentials credentials = new PropertiesCredentials(ExtractRelatedPairs.class.getResourceAsStream("AwsCredentials.properties"));
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
		
		ArrayList<StepConfig> steps = new ArrayList<StepConfig>();
		
		System.out.println("Step1 conf");
		
		// Step 1
		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
		.withJar("s3n://shimritadi/Step1.jar")
		.withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/5gram/data","s3n://shimritadi/PartA/output/A-Final/");
		//"s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/5gram/data"
		//"s3n://dsp112/eng.corp.10k"
		
		StepConfig stepConfig1 = new StepConfig()
			.withName("Step1")
			.withHadoopJarStep(hadoopJarStep1)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		// Step 2
		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
		.withJar("s3n://shimritadi/Step2.jar")
		.withArgs("s3n://shimritadi/PartA/output/A-Final/","s3n://shimritadi/PartA/output/B-Final/");
		
		StepConfig stepConfig2 = new StepConfig()
			.withName("Step2")
			.withHadoopJarStep(hadoopJarStep2)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		// Step 3
		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
		.withJar("s3n://shimritadi/Step3.jar")
		.withArgs("s3n://shimritadi/PartA/output/B-Final/","s3n://shimritadi/PartA/output/C-Final/");
		
		StepConfig stepConfig3 = new StepConfig()
			.withName("Step3")
			.withHadoopJarStep(hadoopJarStep3)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		// Step 4
		HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
		.withJar("s3n://shimritadi/Step4.jar")
		.withArgs("s3n://shimritadi/PartA/output/C-Final/","s3n://shimritadi/PartA/output/D-Final/");

		
		StepConfig stepConfig4 = new StepConfig()
			.withName("Step4")
			.withHadoopJarStep(hadoopJarStep4)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		// Step 5
		HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
		.withJar("s3n://shimritadi/Step5.jar")
		.withArgs("s3n://shimritadi/PartA/output/D-Final/","s3n://shimritadi/PartA/output/E-Final/"); 
		
		StepConfig stepConfig5 = new StepConfig()
		.withName("Step5")
		.withHadoopJarStep(hadoopJarStep5)
		.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		
		System.out.println("def jobflow");
		
		steps.add(stepConfig1);
		steps.add(stepConfig2);
		steps.add(stepConfig3);
		steps.add(stepConfig4);
		steps.add(stepConfig5);
		
		System.out.println("open instances");
		
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		    .withInstanceCount(15)
		    .withMasterInstanceType(InstanceType.M1Medium.toString())
		    .withSlaveInstanceType(InstanceType.M1Medium.toString())
		    .withHadoopVersion("2.4.0")
		    .withEc2KeyName("DSP-Task2")
		    .withKeepJobFlowAliveWhenNoSteps(false)
		    .withPlacement(new PlacementType("us-east-1b"));
		
		
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
			.withAmiVersion("3.1") 
		    .withName("ExtractRelatedPairs")
		    .withInstances(instances)
		    .withSteps(steps)
		    .withLogUri("s3n://shimritadi/Logs/");
		 
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		
		System.out.println("Ran job flow with id: " + jobFlowId);
	}

}
