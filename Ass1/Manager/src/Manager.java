import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class Manager {
	
	public static class Global{
		
		  public static int numOfLocalApps = 0;
		  public static List<List< String[] >> doneTasks = new ArrayList<List< String[] >>();
		  public static String bucketName="";
		  public static String manager_tasks_sqs="";
		  public static String done_tasks_sqs="";
		  public static String workers_tasks_sqs="";
		  public static String workers_response_sqs="";
		  
	}
	
	
	public static void main(String[] args){
		
		boolean terminated = false;
		
		try{
	        AWSCredentials credentials = new PropertiesCredentials(Manager.class.getResourceAsStream("AwsCredentials.properties"));
			AmazonEC2 ec2 = new AmazonEC2Client(credentials);
			AmazonS3 s3 = new AmazonS3Client(credentials);
			AmazonSQS sqs = new AmazonSQSClient(credentials);
		
	
	        System.out.println("manager: find the LocalApp->Manager SQS");
			Global.manager_tasks_sqs = sqs.getQueueUrl("local_manager_sqs").getQueueUrl(); 

			System.out.println("manager: find the Manager->LocalApp SQS");
			Global.done_tasks_sqs = sqs.getQueueUrl("manager_local_sqs").getQueueUrl();
			

			System.out.println("manager: create the Manager-> workers");
			Global.workers_tasks_sqs = sqs.createQueue(new CreateQueueRequest("manager_workers_sqs")).getQueueUrl();
			
	        System.out.println("manager: create the workers->Manager SQS");
	        Global.workers_response_sqs = sqs.createQueue(new CreateQueueRequest("workers_manager_sqs")).getQueueUrl();
	        
	        System.out.println("manager: download messages from localApp -> Manager sqs");
			List<Message> messages = sqs.receiveMessage(Global.manager_tasks_sqs).getMessages();

			while(true){
				if( !( terminated && messages.size()==0 ) ) 
				{
					for(Message message: messages)
					{
						Global.numOfLocalApps++;
						
						//each message from localApp to manager contains the body  "LocalID ; bucketName ; n ; terminate/---"
						String[] splitMessages = message.getBody().split(";");
						Global.bucketName = splitMessages[1];
						
						if(splitMessages[3] .equals("_terminate"))
						{
							System.out.println("manager: received terminated message");	
							terminated = true;
						}
						System.out.println("manager: Download inputfile from s3");
						File inputFile = new File("inputFile");
						s3.getObject(new GetObjectRequest(splitMessages[1], splitMessages[0] + "_inputFile"),inputFile);
						
						//***create SQS message for each URL***
						//parse tasks from local_manager_sqs					
						String[][]tasks = filetoString(inputFile);
						//add more workers if necessary
						createWorkers((int)Math.ceil((double)tasks.length/Integer.parseInt(splitMessages[2])),ec2);
						//send message to the Manager->workers sqs - each message will contain the body "LocalID ; numOftasks ; operation;URL ; bucketName"
						System.out.println("manager: send message to workers");
						for(String[] operation_Url: tasks){
				            sqs.sendMessage(new SendMessageRequest(Global.workers_tasks_sqs, splitMessages[0]+ ";"+ tasks.length +";"+ operation_Url[0] +";" + operation_Url[1] + ";" + splitMessages[1]));
						}
						System.out.println("manager deleting message");
						String messageRecieptHandle = message.getReceiptHandle();
			            sqs.deleteMessage(new DeleteMessageRequest(Global.manager_tasks_sqs, messageRecieptHandle));
					}
					System.out.println("manager: check for done tasks");
					checkDoneTasks(s3,sqs);
					messages = sqs.receiveMessage(Global.manager_tasks_sqs).getMessages();
				}
				else{
					break;
				}
			}
			terminateWorkers(ec2,s3, sqs);
			
		}
		catch(AmazonServiceException ase){
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        }
		catch(AmazonClientException ace){
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } catch(IOException e){
			e.printStackTrace();
		} 
	}
	
	
	
	
/*****
	converts S3 object file to string[][] where string[]=task & string[][]=URL   
*****/
	private static String[][] filetoString(File input){ 
		String[][] ParsedTasks = null;
		try{
			BufferedReader in = new BufferedReader(new FileReader(input));
			int lines = 0;
			List<String> tasks = new ArrayList<String>();
			String[] temp;
			String task;
			while ((task = in.readLine()) != null) {lines++; tasks.add(task);}
			ParsedTasks = new String[tasks.size()][2];
			lines = 0;
			for(String Parsedtask : tasks){
				temp = Parsedtask.split("\t");
				ParsedTasks[lines][0] = temp[0];
				ParsedTasks[lines][1] = temp[1];
				lines++;
			}
			in.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return ParsedTasks;
	}
	
	
	
/*****
	creates and runs (numOfWorkers - active workers)  new instances with the tag  "Worker"     
 *****/
	private static void createWorkers(int workersRequired,AmazonEC2 ec2){
		System.out.println("manager: check if more workers needed");
		int workersToAdd,activeWorkers = 0;
		List<Instance> instances = new ArrayList<Instance>();
		List<Reservation> reservations = ec2.describeInstances().getReservations();		
		for(Reservation res : reservations){
			instances = res.getInstances();
			for(Instance instance:instances){	
				if(instance.getTags().get(0).getValue().equals("Worker") && instance.getState().getName().equals("running")){
					activeWorkers++;
		         }
			}
		}
		workersToAdd = workersRequired-activeWorkers;
		if(workersToAdd > 0){
			System.out.println("manager: adds " + workersToAdd + " workers");
			RunInstancesRequest request = new RunInstancesRequest("ami-51792c38", workersToAdd, workersToAdd);
	        request.setInstanceType(InstanceType.T1Micro.toString());
	        request.withKeyName("DSP-Task2");
	        request.withUserData(encodedWorkerData());
	        Reservation newWorkersInstances = ec2.runInstances(request).getReservation();
	        for(Instance instance: newWorkersInstances.getInstances()){
	        	ec2.createTags(new CreateTagsRequest().withResources(instance.getInstanceId()).withTags(new Tag("Worker","Worker")));
	        }
	    }	
	}

	
	
/*****
	encode the Worker code (uploaded into S3bucket) to base64       
 *****/
	
	private static String encodedWorkerData(){
        String encodedData = new String(Base64.encodeBase64(("#! /bin/bash\n" + "cd /\n" +
        		"wget https://s3.amazonaws.com/AdiShimritBucket/worker.zip\n"+
        		"unzip -P shimritadi worker.zip\n" +
        		"java -jar worker.jar >& 1.log" ).getBytes()));
        return encodedData;
	}

	
	
/*****
	goes through all messages in the workers_response_sqs and calls the createSumFile for each time
	that the workers responded to all input file of some LocalApp          
*****/
		
	private static void checkDoneTasks(AmazonS3 s3, AmazonSQS sqs){
		String[] splitMessages;
		
		while(true){
			// receiving one message from worker->manager queue
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(Global.workers_response_sqs).withMaxNumberOfMessages(1);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			
			if(messages.size()==0){
				return;
			}
			Message message = messages.get(0);
			
			//delete message from the Worker->Manager SQS
			String messageRecieptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(Global.workers_response_sqs, messageRecieptHandle));
            
            //each message contains : LocalID | numOftasks | originalURL | S3URL/exception details |operation | exception?
			splitMessages = message.getBody().split(";");
			String currID = splitMessages[0];
			boolean contains = false;
			
			for(List<String[]> doneTask : Global.doneTasks)
			{
				if(doneTask.get(0)[0].equals(currID))
				{
					doneTask.add(splitMessages);
					contains = true; 
					break;
				}
			}
			if(!contains)
			{
				List<String[]> currIDList = new ArrayList<String[]>();
				currIDList.add(splitMessages);
				Global.doneTasks.add(currIDList);
			}
			
			//checks if any task is done
			for( int i= 0; i < Global.doneTasks.size() ; i++ )
			{
				if( Global.doneTasks.get(i).size() == Integer.parseInt( Global.doneTasks.get(i).get(0)[1]) )
				{
					System.out.println("manager: **COMPLETE TASK FOR LOCAL**");
					createSumFile(s3, sqs,Global.doneTasks.get(i));
					Global.doneTasks.remove(Global.doneTasks.get(i));
				}
			}
		}
	}
	

/*****
	create summary file upload it to S3 and send done task message to the done_tasks_sqs          
*****/
			
	private static void createSumFile(AmazonS3 s3 ,AmazonSQS sqs,List<String[]> sumTasks){
		try{
			System.out.println("manager: creats summary file");
			File outputFile = new File("outputfile");
			BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
			for(String[] doneTask: sumTasks){
				//doneTask = LocalID | numOftasks | originalURL | S3URL/exception details |operation | exception? 
				if(doneTask[5].equals( "_exceptionMessage")){
				out.write("< " + doneTask[4] +" >: "+ "\t" + doneTask[2] +  "\t< " + doneTask[3]+ " >" + "\t" + "_exceptionMessage");
				}
				else{
					out.write("< " + doneTask[4] +" >: "+ "\t" + doneTask[2] +  "\t" + doneTask[3] + "\t" +  " --- ");
				}
				out.newLine();
			}
			out.close();
			String currLocalID = sumTasks.get(0)[0];
			System.out.println("manager: uploads summaryfile to s3");
            s3.putObject(new PutObjectRequest(Global.bucketName, currLocalID +"_summaryFile", outputFile).withCannedAcl(CannedAccessControlList.PublicRead));
            
            System.out.println("manager: sends done task message to the localapp");
            sqs.sendMessage(new SendMessageRequest(Global.done_tasks_sqs,currLocalID));
            Global.numOfLocalApps--;
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

/*****
	closing LocalApp->Manager , manager->workers and workers->manager queues, all workers instances and sending terminated message to LocalApp   
*****/
	private static void terminateWorkers(AmazonEC2 ec2,AmazonS3 s3, AmazonSQS sqs){
		
		System.out.println("closing LocalApp->Manager queue");
		sqs.deleteQueue(Global.manager_tasks_sqs);
		
		while(Global.numOfLocalApps > 0)
		{
			checkDoneTasks(s3, sqs);
		}
		
		System.out.println("manager: closing manager->workers queue");
		sqs.deleteQueue(Global.workers_tasks_sqs);
		
		System.out.println("manager: closing workers->manager queue");
		sqs.deleteQueue(Global.workers_response_sqs);
		
		List<Instance> instances = new ArrayList<Instance>();
		List<String> instancesID = new ArrayList<String>();
		List<Reservation> reservations = ec2.describeInstances().getReservations();		
		for(Reservation res : reservations){
			instances = res.getInstances();
			for(Instance instance:instances){	
				if(instance.getTags().get(0).getValue().equals("Worker")){
					instancesID.add(instance.getInstanceId());
				}
			}
		}
		System.out.println("manager: shutting down all workers instances");
		ec2.terminateInstances(new TerminateInstancesRequest(instancesID));
		
		System.out.println("manager: sends terminate message to LocalApp");
		sqs.sendMessage(new SendMessageRequest(Global.done_tasks_sqs,"_terminated"));
		
		System.out.println("manager: SHUTTING DOWN...");
	}
	
}


