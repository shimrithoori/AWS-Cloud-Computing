package dataStructure;

import java.util.LinkedList;
import java.util.List;

public class StopWords {
	
	public static List<String> stopWords;

	public StopWords() {
		
		String[] stopWordsArray = { "I","the","a","A","an","An","about","above","after","again","against","all","am","an","and","any","are","aren't","as","at","b",
		"B","C","D","E","F","G","H","I","J","K", "L", "M", "N" , "O" ,"P", "Q", "R", "S", "T", "U", "V" , "W", "X", "Y","Z","be","because",
		"been","before","being","below","between","both","but","by","c","can't","cannot","could","couldn't","d","did","didn't","do","does","doesn't","doing",
		"don't","down","during","e","each","f","few","for","from","further","g","h","had","hadn't","has","hasn't","have","haven't","having","he","He","he'd","he'll",
		"he's","her","here","here's","hers","herself","him","Him","himself","his","His","how","how's","i","i'd","i'll","i'm","i've","if","If","in","into","is","isn't","it","It","it's",
		"its","Its","itself","j","k","l","let's","m","me","more","most","mustn't","my","My","myself","n","no","nor","not","o","O","of","off","on","once","only","or","other",
		"ought","our","ours","ourselves","out","over","own","p","s","same","shan't","she","She","she'd","she'll","she's","should","shouldn't","so","some","such","t",
		"than","that","that's","the","The","their","theirs","them","themselves","then","there","there's","these","they","They","they'd","they'll","they're","they've","this",
		"This","those","through","to","To","too","u","under","until","up","v","very","w","was","wasn't","we","We","we'd","we'll","we're","we've","were","weren't","what","what's",
		"when","When","when's","where","where's","which","while","who","who's","whom","why","why's","with","won't","would","wouldn't","y","you","You","you'd","you'll","you're",
		"you've","your","yours","yourself","yourselves","'","^","?",";",":","1",".","-","*","#","$","&","%","!",")","(","•" };
		stopWords = new LinkedList<String>();
		for(String str : stopWordsArray) {
			stopWords.add(str);
		}
	
	}

	public boolean contains(String ostr) {
		
		for(String str: stopWords) {
			if(str.compareTo(ostr) == 0) {
				return true;
			}
		}
		return false;

	}
	
}
	