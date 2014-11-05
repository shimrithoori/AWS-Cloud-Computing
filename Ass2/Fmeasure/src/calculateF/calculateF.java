package calculateF;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;

import dataStructure.PMIwordPair;
import dataStructure.TestSet;
import dataStructure.WordPair;

public class calculateF {
	
	public static void main(String[] args) throws Exception { 
		List<PMIwordPair> list = new LinkedList<PMIwordPair>();
 		double tp=0, fp=0, fn=0;
		double fMeasure=0, precision, recall, newfMeasure; 
		String line;
		TestSet testset = new TestSet();
		
//		double threshold = Double.parseDouble(args[0]);
		double threshold = 0.2; //debug
		
		FileReader inputFile = new FileReader("C:/Users/user/Desktop/DSP/Ass2/submission/FinalOutput/PartBpairs.txt"); 
		BufferedReader br = new BufferedReader(inputFile);

		while((line = br.readLine()) != null) {
			String[] parsedKeyVal = line.split("\t");
			WordPair wordpair = new WordPair(parsedKeyVal[0].split(",")[0].toString(), parsedKeyVal[0].split(",")[1].toString(),10);
			double pmi = Double.parseDouble(parsedKeyVal[1]);
			if(testset.contains(wordpair)) {
				list.add(new PMIwordPair(pmi, wordpair));
			}
		}
		br.close();
		inputFile.close();
		
		int i = 0;
		calibrationLoop:
		while(i<30){
			for(PMIwordPair wp: list) {
				if(wp.PMI.get() >= threshold) { //related according to threshold
					if(testset.isRelated(wp.wordpair)) { //related according to testSet
						tp++;
					}else{
						fp++;
					}
				}else if(wp.PMI.get() < threshold) { //not related according to threshold
					if(testset.isRelated(wp.wordpair)) {
						fn++;
					}
				}
			}
			precision = tp/(tp+fp);
			recall = tp/(tp+fn);
//			System.out.println("tp="+tp +"	fp="+fp +"	fn="+fn);
//			System.out.println("prec="+precision + "  recall="+recall);
			
			i++;
			newfMeasure= 2*(precision*recall)/(precision+recall);
//			System.out.println("newF= "+ newfMeasure + " F= "+ fMeasure);
			
			if(newfMeasure >= fMeasure) {
				fMeasure= newfMeasure;
				System.out.println("F-measure is: " + fMeasure+ "( threshold is: " + threshold+" )");
				threshold += 0.1; //?
				tp=fp=fn=newfMeasure=0;
			}
			else break calibrationLoop;
		}
	}
}
