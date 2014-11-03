import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class LocalApp {
	
	public static class Global{
		
		  public static String id ;
		  public static boolean terminator ;
		  public static String managerLocalUrl, localManagerUrl, bucketName ;
	}
	
    public static void main(String[] args){ 
    	
    	// Create a unique LocalId for LocalApp
    	Global.id = UUID.randomUUID().toString() ;
    	System.out.println("LocalApp; LocalId is: " + Global.id );
    	Global.terminator = false ;

    	try{
	    	AWSCredentials credentials = new PropertiesCredentials(LocalApp.class.getResourceAsStream("AwsCredentials.properties"));
	    	Global.bucketName = credentials.getAWSAccessKeyId().toLowerCase(); 
	    	
	    	AmazonEC2 ec2 = new AmazonEC2Client( credentials) ;
	    	AmazonS3 s3 = new AmazonS3Client( credentials ) ;
	    	AmazonSQS sqs = new AmazonSQSClient( credentials );
	    	
			System.out.println("LocalApp: Creating bucket named " + Global.bucketName + "\n");
			s3.createBucket( Global.bucketName );
			
	    	// Check if a manager already exists, if not- create one
	    	if( !isManagerExist( ec2 ) )
	    	{
	    		System.out.println("LocalApp: Initialazing new Manager...") ;
	    		createManager( ec2 ) ; 
	    		
				// Creating queues : Global.managerLocalUrl, Global.localManagerURl
	    		createSQSqueues( sqs );
	
	    	}
	    	else{
	    		// in case Manager is terminating, the local->manager queue not exists
	    		System.out.println("LocalApp: manager exists");
	    		// assuming that if manager exists, the queues exists
				Global.managerLocalUrl = ( sqs.getQueueUrl( "manager_local_sqs" ).getQueueUrl() ) ; 
	    		Global.localManagerUrl = ( sqs.getQueueUrl( "local_manager_sqs" ).getQueueUrl() ) ;
	    	}
	    	
			// Uploading input file to bucket - with the name "LocalId_inputFile"
			File inputFile = new File(args[0]) ;
			uploadFileToS3( s3 , inputFile );
	
	        // Send local->manager queue a massage
	        System.out.println("LocalApp: Sending a message to local_manager_sqs queue.\n");
	        
	        // message body: [ localId;bucketName;n;termination/"---" ] 
	        String newTaskMessageBody ;
	        
	        // check if we got termination argument
	        if( args.length == 4 ){
				newTaskMessageBody = Global.id + ";" + Global.bucketName + ";" + args[2] + ";" + "_terminate" ; 
				System.out.println("---------- LocalApp: Im the TERMINATOR ----------\n");
				Global.terminator = true; 
	        }
	        else{
	        	 newTaskMessageBody = Global.id + ";" + Global.bucketName + ";" + args[2]+ ";"+ "---" ; 
	        }
	        sqs.sendMessage(new SendMessageRequest( Global.localManagerUrl, newTaskMessageBody));	
	
	        System.out.println("LocalApp: Message was sent to local_manager_sqs queue - - - - Local is waiting for job to be done") ;
	        waitForDone( ec2, sqs ); 
	        
	        // downloading summary file from s3 - the Manager named the summary file "localId_summaryFile" 
	        List<String> summaryStringList = downloadSummaryFile( s3 ) ;
	        
	        // Creating HTML file from the summary string list
	        String outputFileName = args[1] ;
	        System.out.println("LocalApp: creating html from summary file");
	        createHtmlFile( summaryStringList, outputFileName );  
	        
	        
	        System.out.println("LocalApp: " + Global.id +" SHUTTING DOWN . . . ");
	        // terminating
			return;
		// In case local start working exactly when Manager is terminating
        }catch (QueueDoesNotExistException e){
           	System.out.println("LocalApp: Shutting down --- Manager is in the middle of termination... Try Again");
        } 
    	catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } catch (IOException e) {
			e.printStackTrace();
		}
    }
	
	/*****
    	Creating a manager, if it doesn't exists
    *****/
	private static boolean isManagerExist(AmazonEC2 ec2)
	{
		System.out.println("LocalApp: Checking if manager exists...");
		boolean foundManager = false;
		List<Instance> instances = new ArrayList<Instance>() ;
		List<Reservation> reservations = ec2.describeInstances().getReservations() ;
		
		foundLoop:
			
		for(Reservation res : reservations){
			instances = res.getInstances();
			for(Instance instance : instances){	
				if(instance.getTags().get(0).getValue().equals("Manager") && instance.getState().getName().equals("running")){
					foundManager = true;
					break foundLoop;
				}
			}
		}
		return foundManager ;
	}
	
	/*****
		Creating a Manager 
	*****/
	private static void createManager( AmazonEC2 ec2 )
	{
        	List<Instance> instances = new ArrayList<Instance>(); 
            RunInstancesRequest request = new RunInstancesRequest( "ami-51792c38", 1, 1 );
            request.setInstanceType( InstanceType.T1Micro.toString() );
            request.withUserData( encodedManagerData() ) ;
            request.withKeyName( "DSP-Task2" ) ;
            instances = ec2.runInstances( request ).getReservation().getInstances() ;
            
            // Tag Manager
            System.out.println("LocalApp: Tagging Manager");
            ec2.createTags(new CreateTagsRequest().withResources(instances.get(0).getInstanceId()).withTags(new Tag("Manager","Manager")));

            System.out.println("LocalApp: A new Manager is created...");
	}
	
	/*****
		Creating 2 queues (if manager not exists) : local_manager_sqs and manager_local_sqs
	*****/
	private static void createSQSqueues( AmazonSQS sqs)
	{
		//this list will hold the manager->local url & local->manager url
    	//List<String> queueUrlList = new ArrayList<String>();

		//String localManagerUrl, managerLocalUrl ;
		
		// manager->local queue - for Done task message from Manager to Local 
		Global.managerLocalUrl = sqs.createQueue( new CreateQueueRequest( "manager_local_sqs" ) ).getQueueUrl() ;
		System.out.println("Created manager_local_sqs " + "URL: " + Global.managerLocalUrl );

		// local->manager queue- for New task message from local to manager
		Global.localManagerUrl = sqs.createQueue( new CreateQueueRequest( "local_manager_sqs") ).getQueueUrl() ;
		System.out.println("Created local_manager_sqs " + "URL: " + Global.localManagerUrl);
	}
	
	/*****
		Encode the Manager code (uploaded into S3bucket) to base64       --- edit 
	*****/

	private static String encodedManagerData()
	{
	    String encodedData = new String( Base64.encodeBase64( ( "#! /bin/bash\n" + "cd /\n" +
			"wget https://s3.amazonaws.com/AdiShimritBucket/manager.zip\n" +
			"unzip -P shimritadi manager.zip\n" +
			"java -jar manager.jar >& log.1" ).getBytes() ) ) ;
	    
	    return encodedData;
	 }

	
    /*****
		Upload the input file to S3 bucket
    *****/
	private static void uploadFileToS3( AmazonS3 s3, File file)
	{
    	String inputFileName = Global.id + "_inputFile" ;
    	System.out.println("Uploading -" + inputFileName + "- to S3 bucket -" + Global.bucketName + "-\n");
    	s3.putObject( new PutObjectRequest( Global.bucketName, inputFileName, file ).withCannedAcl( CannedAccessControlList.PublicRead ) ) ;     
    }
   
    /***
    	Waiting to receive a Done message from manager 
    	if got one, delete it and move on  		
    	if in Manager->Local queue does not exist anymore => Manager terminated=> stop waiting
	 ***/
	private static void waitForDone( AmazonEC2 ec2, AmazonSQS sqs )
	{	
    	
    	boolean terminate = false;
    	boolean receivedDoneTask = false ;
    	
    	recieveDoneTaskMsg:
    	while(true){
    		String[] parsedMsg;
    		List<Message> messagesList = sqs.receiveMessage(new ReceiveMessageRequest(Global.managerLocalUrl)).getMessages();
    		
    		// if the queue is empty and Manager terminated => localApp stop waiting
    		if( Global.terminator && terminate && receivedDoneTask )
    		{
    			System.out.println("LocalApp: Getting ready to terminate... ");
				getReadyToTerminate( ec2, sqs );
				return ;
    		}
    		
    		System.out.println("LocalApp: received list of messages from Manager->local qeueu, list size: "+ messagesList.size());
    		
    		// Manager->Local queue exists ( manager is not terminated )
    		for(Message msg : messagesList){
    			// parsing message by ";" - parsedMsg[0] is id of localApp
    			parsedMsg = msg.getBody().split(";");
    			if( Global.terminator && parsedMsg[0].equals("_terminated"))
    			{ 
    				System.out.println("LocalApp: --------------setting TERMINATED flag to TRUE--------------- ");
    				terminate = true ;
    				 
    				System.out.println("LocalApp: deleting terminate message from manager_local queue" );
					String messageRecieptHandle = msg.getReceiptHandle();
		            sqs.deleteMessage(new DeleteMessageRequest( Global.managerLocalUrl, messageRecieptHandle) );
    			}
    			if( parsedMsg[0].equals( Global.id ) )
    			{
    				System.out.println("LocalApp: --------------Received DONE TASK Message from Manager------------- ") ;
    				receivedDoneTask = true ; 
    				
    				System.out.println("LocalApp: deleting done-task message from manager_local queue" );
					String messageRecieptHandle = msg.getReceiptHandle();
		            sqs.deleteMessage(new DeleteMessageRequest( Global.managerLocalUrl, messageRecieptHandle) );
		            
		           // if terminator, go back to while
		           if( !Global.terminator )
		            	break recieveDoneTaskMsg ;
    			}
    		}
    	}
    }
    
    /*****
		Once localApp received Termination message && Manager->local queue is empty:
		localApp takes all the massages from Manager->local queue,
    *****/
	private static void getReadyToTerminate( AmazonEC2 ec2, AmazonSQS sqs )
    {
    	System.out.println("\nLocalApp: **Terminating**\n");
    	
    	// terminating manager instance
    	System.out.println("LocalApp: searching fot Manager instance");
		List<Instance> instances = new ArrayList<Instance>() ;
		List<Reservation> reservations = ec2.describeInstances().getReservations() ;
		
		foundLoop:
		for( Reservation res : reservations) 
		{
			instances = res.getInstances();
			for( Instance instance : instances )
			{	
				if( instance.getTags().get(0).getValue().equals("Manager") && instance.getState().getName().equals("running") )
				{
					System.out.println("LocalApp: deleting Manager inatance");
					List<String> instanceId = new ArrayList<String>();
					instanceId.add( instance.getInstanceId());
					ec2.terminateInstances( new TerminateInstancesRequest( instanceId) ) ;
					break foundLoop ;
				}
			}

		}
		
    		
	}

	/***
    	Downloading the summary file from s3 and converting it to string list 
     ***/
	private static List<String> downloadSummaryFile( AmazonS3 s3 ) throws IOException
	{
		String summaryFileName = Global.id + "_summaryFile";
    	S3Object object  = s3.getObject(new GetObjectRequest( Global.bucketName, summaryFileName) );
    	System.out.println("Summary file is downloaded from s3...") ;
    	
		// Converting the summary file to String list
		BufferedReader reader = new BufferedReader( new InputStreamReader( object.getObjectContent() ) ) ;
		List<String> summaryStringList = new ArrayList<String>() ; 
		
		while(true){
			String line = reader.readLine();
			if(line == null) break;
			summaryStringList.add(line) ;
		}
		return summaryStringList;
    }
    
    /*****
    	Creating HTML file from the summary string list
    *****/
	private static void createHtmlFile(List<String> summaryStringList, String outputFileName){
    	try{
    		File outputFile = new File( outputFileName ) ;
    		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile)); //*****getAbsoluteFile??***
    		writer.write("<html>");
    		writer.write("<body>");
    		
    		System.out.println("LocalApp: summaryStringList size is: " + summaryStringList.size() );
    		for(String str : summaryStringList){
    			String[] splitedLine = str.split("\t");
    			
    			// splitedLine: < operation > | input file Link	| output file / <exception> | exception ?    
    			if( splitedLine[3].equals("_exceptionMessage")){ // check if there was exception
    				writer.write(splitedLine[0] + "<a href=" + splitedLine[1] +">inputFileLink</a> "+ splitedLine[2] + "<br>");
    			}
    			else{
    				writer.write( splitedLine[0] + "<a href=" + splitedLine[1] +">inputFileLink</a> <a href="+ splitedLine[2]+ ">outputFileLink</a>" + "<br>" );
    			}
    		}
    		writer.write("</body>");
    		writer.write("</html>");
    		writer.close();
    		System.out.println("done with " + outputFileName);
    	}catch (IOException e) {
			e.printStackTrace();
    	} 
    }
    
}
