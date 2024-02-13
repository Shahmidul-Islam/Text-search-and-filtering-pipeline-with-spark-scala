package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;

/*
 * This class has a single function that returns the term count when given a term and a content of a document as a List
 * */
 

public class TermFrequencyCounter {
	
	public short calculateTermFrequency(String term, List<String> content) {
		
		short freq = 0;
		
		for(String s:content) {
			if (term.equalsIgnoreCase(s)) freq +=1;
		}
		return freq;
	}

}
