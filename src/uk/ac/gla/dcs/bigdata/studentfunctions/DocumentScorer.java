package uk.ac.gla.dcs.bigdata.studentfunctions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.*;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.CollectionTermFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryParser;
import scala.Tuple2;

public class DocumentScorer implements MapGroupsFunction<Query,QueryParser, DocumentRanking>{

	/**
	 * This a mapGroups function that groups each QueryParser Object by Query depending on the dph score calculated
	 * It returns a list of type DocumentRanking
	 */
	private static final long serialVersionUID = 1L;
	
	Broadcast<Double> averageDocumentLengthInCorpusBroadcast;
	Broadcast<Long> totalDocsInCorpusBroadcast;
	Broadcast<List<Tuple2<Query,CollectionTermFrequency>>> collectionFreqBroadcast;
	
	

	public DocumentScorer(Broadcast<Double> averageDocumentLengthInCorpusBroadcast,
			Broadcast<Long> totalDocsInCorpusBroadcast,
			Broadcast<List<Tuple2<Query, CollectionTermFrequency>>> collectionFreqBroadcast) {
		super();
		this.averageDocumentLengthInCorpusBroadcast = averageDocumentLengthInCorpusBroadcast;
		this.totalDocsInCorpusBroadcast = totalDocsInCorpusBroadcast;
		this.collectionFreqBroadcast = collectionFreqBroadcast;
	}


	@Override
	public DocumentRanking call(Query query, Iterator<QueryParser> queryParser) throws Exception {
		
		//Get the broadcast variables for scoring
		long totalDocsInCorpus = totalDocsInCorpusBroadcast.value();
		double averageDocLegthInCorpus = averageDocumentLengthInCorpusBroadcast.value();
		List<Tuple2<Query,CollectionTermFrequency>> collectionFreqList = collectionFreqBroadcast.value();
		
		//Creates an empty list of Ranked result to store the results of the ranking
		List<RankedResult> rankedList = new ArrayList<RankedResult>();
		
		//Loops through parsed queries and calculates the dph score
		
		while(queryParser.hasNext()) {
			//store the current queryParser object
			QueryParser parser = queryParser.next();
			
			int documentLength = parser.getDocumentLength();
			//creates a collection frequency array 
			int[] collectionFrequency = new int[query.getQueryTermCounts().length];
			
			//loops through the list of collection term frequencies and match with the current query
			for(int i=0; i<collectionFreqList.size();i++) {
				if(query.getOriginalQuery().equalsIgnoreCase(collectionFreqList.get(i)._1.getOriginalQuery())) {
					 collectionFrequency = collectionFreqList.get(i)._2.getFreq();
				}
			}
			
			int sizeOfQuery = parser.getTermFrequency().length;
			//Initialize the score
			double score = 0.0;
			
			//Loops through all terms in a query and calculate average dph score 
			for(int i=0; i<sizeOfQuery;i++) {
				
				short termFrequency = parser.getTermFrequency()[i];
				
				if(termFrequency != 0) {
					
					int totalTermFrequencyInCorpus = collectionFrequency[i];
					
					score += DPHScorer.getDPHScore(termFrequency,
							totalTermFrequencyInCorpus,
							documentLength, 
							averageDocLegthInCorpus, 
							totalDocsInCorpus);
				}
			}
			if (score !=0.0) {
				score = score / sizeOfQuery;
				
			}
			//creates an object of ranked result
			RankedResult res = new RankedResult();
			res.setArticle(parser.getArticle());
			res.setDocid(parser.getArticle().getId());
			res.setScore(score);
			
			//adds the ranked result to the list
			rankedList.add(res);
			
		}
		//collect the ranked result as a list
		Collections.sort(rankedList);
		Collections.reverse(rankedList);
		
		//collect the top 10 documents
		List<RankedResult> results = new ArrayList<RankedResult>();
		results = rankedList.subList(0, 10);
		
		//Redundancy Filtering removes overly similar documents using the text distance calculator
		int next = 10;
		
		for(int i=0;i<10;i++) {
			
			RankedResult res = results.get(i);
			
			String firstTitle = res.getArticle().getTitle();
			
			//Skips empty titles
			if(firstTitle==null) {
				continue;
			}
			
			for(int j=i+1;j<results.size();j++) {
				
				String secondTitle = results.get(j).getArticle().getTitle();
				if(secondTitle==null) {
					continue;
				}
				
				double distance = TextDistanceCalculator.similarity(firstTitle, secondTitle);
				
				//if the distance between the titles is less than 0.5, the second article is removed.
				if(distance<0.5) {
					
					results.remove(j);
				}
			}
			//Maintains the number of documents in the result set to 10 per query
			while (results.size()!=10) {
				results.add(rankedList.get(next));
				next += 1;
			}
			
			
		}
		
		DocumentRanking doc = new DocumentRanking();
		doc.setQuery(query);
		doc.setResults(results);
		return doc;
	}

}
