package dataStructure;

import java.util.LinkedList;
import java.util.List;

import dataStructure.WordPair;

public class TestSet {
	
	public static List<WordPair> relatedPairs;
	public static List<WordPair> unRelatedPairs;

	public TestSet(){
		String stringPosPairs = "tiger	jaguar,tiger	feline,closet	clothes,planet	sun,hotel	reservation,planet	constellation,"
				+ "credit	card,stock	market,psychology	psychiatry,planet	moon,planet	galaxy,bank	money,physics	proton,"
				+ "vodka	brandy,war	troops,Harvard	Yale,news	report,psychology	Freud,money	wealth,man	woman,"
				+ "FBI	investigation,network	hardware,nature	environment,seafood	food,weather	forecast,championship	tournament,"
				+ "law	lawyer,money	dollar,calculation	computation,planet	star,Jerusalem	Israel,vodka	gin,money	bank,"
				+ "computer	software,murder	manslaughter,king	queen,OPEC	oil,Maradona	football,mile	kilometer,seafood	lobster,"
				+ "furnace	stove,environment	ecology,boy	lad,asylum	madhouse,street	avenue,car	automobile,gem	jewel,type	kind,"
				+ "magician	wizard,football	soccer,money	currency,money	cash,coast	shore,money	cash,dollar	buck,journey	voyage,"
				+ "midday	noon,tiger	tiger";
		String[] stringPosPairsArray = stringPosPairs.split(",");
		relatedPairs = new LinkedList<WordPair>();
		for(String str : stringPosPairsArray){
			relatedPairs.add(new WordPair(str.split("\t")[0], str.split("\t")[1],10));
		}
		
		String stringNegPairs = "king	cabbage,professor	cucumber,chord	smile,noon	string,rooster	voyage,sugar	approach,"
				+ "stock	jaguar,stock	life,monk	slave,lad	wizard,delay	racism,stock	CD,drink	ear,stock	phone,"
				+ "holy	sex,production	hike,precedent	group,stock	egg,energy	secretary,month	hotel,forest	graveyard,"
				+ "cup	substance,possibility	girl,cemetery	woodland,glass	magician,cup	entity,Wednesday	news,"
				+ "direction	combination,reason	hypertension,sign	recess,problem	airport,cup	article,Arafat	Jackson,"
				+ "precedent	collection,volunteer	motto,listing	proximity,opera	industry,drink	mother,crane	implement,"
				+ "line	insurance,announcement	effort,precedent	cognition,media	gain,cup	artifact,Mars	water,peace	insurance,"
				+ "viewer	serial,president	medal,prejudice	recognition,drink	car,shore	woodland,coast	forest,century	nation,"
				+ "practice	instituton,governor	interview,money	operation,delay	news,morality	importance,announcement	production,"
				+ "five	month,school	center,experience	music,seven	series,report	gain,music	project,cup	object,"
				+ "atmosphere	landscape,minority	peace,peace	atmosphere,morality	marriage,stock	live,population	development,"
				+ "architecture	century,precedent	information,situation	isolation,media	trading,profit	warning,"
				+ "chance	credibility,theater	history,day	summer,development	issue";
		String[] stringNegPairsArray = stringNegPairs.split(",");
		unRelatedPairs = new LinkedList<WordPair>();
		for(String str : stringNegPairsArray) {
			unRelatedPairs.add(new WordPair(str.split("\t")[0], str.split("\t")[1],10));
		}
	}

	public boolean contains(WordPair wordpair) {
		
		for(WordPair wp: relatedPairs) {
			if(wp.compareTo(wordpair) == 0) {
				return true;
			}
		}
		for(WordPair wp: unRelatedPairs) {
			if(wp.compareTo(wordpair) == 0) {
				return true;
			}
		}
		return false;
	}
	
	public boolean isRelated(WordPair wordpair) {

		for(WordPair wp: relatedPairs) {
			if(wp.compareTo(wordpair) == 0) {
				return true;
			}
		}
		return false;
		
	}
	
}
	



