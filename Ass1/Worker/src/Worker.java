import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.List;
import java.util.UUID;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFImageWriter;
import org.apache.pdfbox.util.PDFText2HTML;
import org.apache.pdfbox.util.PDFTextStripper;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class Worker {

	public static class Global{
		
		  public static String workers_tasks_sqs, workers_response_sqs ;
	}
	
	public static void main(String[] args){
				
		try{
			AWSCredentials credentials = new PropertiesCredentials(Worker.class.getResourceAsStream("AwsCredentials.properties"));
			AmazonS3 s3 = new AmazonS3Client(credentials);
			AmazonSQS sqs = new AmazonSQSClient(credentials);

//find the Manager->Workers SQS 
			Global.workers_tasks_sqs = sqs.getQueueUrl("manager_workers_sqs").getQueueUrl();
		
//find the Workers->Manager SQS 
			Global.workers_response_sqs = sqs.getQueueUrl("workers_manager_sqs").getQueueUrl();
			
			System.out.println(" Worker:  waiting for message from worker_task_sqs...");
			
			while(true)
			{
//receive one message from sqs - each message: "LocalID ; numOftasks ; operation ; URL ; bucketName"
				try{
					ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(Global.workers_tasks_sqs).withMaxNumberOfMessages(1);
					System.out.println(" Worker: request messeges from  workers_tasks_sqs");
	//insert the above message to List with type Message 				
					List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
 
					if( messages.size() > 0 )
					{
						System.out.println(" Worker: check if got messages");
						Message message = messages.get(0);
						//set the message visibility timeout to 3 minute
						sqs.changeMessageVisibility(Global.workers_tasks_sqs, message.getReceiptHandle(), 180 ) ;
						
						String[] splitMessages = message.getBody().split(";");
						
						//Perform Operation on PDF File and upload it to S3
						System.out.println(" Worker: Performing operation");
						PerformOperation( s3, sqs, splitMessages );
						
						System.out.println(" Worker: Deleting message from manager->worker queue");
						String messageRecieptHandle = message.getReceiptHandle();
			            sqs.deleteMessage(new DeleteMessageRequest(Global.workers_tasks_sqs, messageRecieptHandle));
					}	
				}
				catch (QueueDoesNotExistException e){
		           	System.out.println("worker: SHUTTING DOWN");
		           	return;
		        }
			}
		}
		
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        }
		catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } 
		catch (IOException e) {
			e.printStackTrace();
		} 

	}
	
	private static void PerformOperation(AmazonS3 s3, AmazonSQS sqs, String[] message)
	{
		try{
			File file;
			String nameOfFile;
			System.out.println(" Worker: Loading input file from URL to PDF ");
			PDDocument PDFfile = PDDocument.load(new URL(message[3]));
			if( message[2].equals("ToImage") )
			{
				System.out.println(" Worker: operation is ToImge");
				PDFImageWriter PDFimage = new PDFImageWriter();
				PDFimage.writeImage(PDFfile,"png", null,1,1,nameOfFile = "IMAGE"+ UUID.randomUUID());
				nameOfFile = nameOfFile + "1.png";
				file = new File(nameOfFile);
			}
			else if( message[2].equals("ToHTML") )
			{
				System.out.println(" Worker: operation is ToHtml ");
				PDFText2HTML PDFHtml = new PDFText2HTML("UTF-8"); 
				PDFHtml.writeText(PDFfile, new PrintWriter(new FileWriter(file = new File(nameOfFile ="HTML" + UUID.randomUUID() + ".html"))));
			}
				
			else if( message[2].equals("ToText") )
			{
				System.out.println(" Worker: operation is ToText");
				PDFTextStripper PDFtxt = new PDFTextStripper("UTF-8");
				PDFtxt.writeText(PDFfile, new PrintWriter(new FileWriter(file = new File(nameOfFile ="TEXT"+ UUID.randomUUID() + ".txt"))));
			}
			else
			{
				System.out.println(" Worker: Invalid opeation");
				sendMessage(sqs, message, "Invalid operation", "_exceptionMessage");
				PDFfile.close();
				return;
			}
			
			PDFfile.close(); 
			
			if(!new File(nameOfFile).exists()){
				System.out.println(" Worker: Corrupted File ");
				sendMessage(sqs, message,"Corrupted File", "_exceptionMessage");
				return;
			}
			System.out.println(" Worker: Uploading to S3 ");
			s3.putObject(new PutObjectRequest(message[4], nameOfFile, file).withCannedAcl(CannedAccessControlList.PublicRead));
		 
			System.out.println(" Worker: ******** Sending a message to worker_response_sqs *********  ");
			sendMessage(sqs, message, "https://s3.amazonaws.com/"+message[4]+"/"+nameOfFile , "---");
			
		}catch (IOException e) {
			sendMessage(sqs, message, e.toString(), "_exceptionMessage");
		}catch (NoClassDefFoundError e) {
			sendMessage(sqs, message, e.toString(), "_exceptionMessage");
		}
	}
	
	
	/*****
	Send a message to worker->manager queue
	message body: LocalID | numOftasks | originalURL | S3URL |operation | exception?
	*****/
	private static void sendMessage(AmazonSQS sqs, String[] message, String content, String err)
	{
		sqs.sendMessage(new SendMessageRequest( Global.workers_response_sqs , message[0] + ";"
				+ message[1] + ";"
				+ message[3] + ";"
				+ content + ";"
				+ message[2]+ ";"
				+ err ));
	}
}
