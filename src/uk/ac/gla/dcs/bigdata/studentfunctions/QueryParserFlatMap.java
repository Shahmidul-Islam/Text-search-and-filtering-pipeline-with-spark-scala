package uk.ac.gla.dcs.bigdata.studentfunctions;
import uk.ac.gla.dcs.bigdata.providedutilities.*;
import java.util.*;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.*;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryParser;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

public class QueryParserFlatMap implements FlatMapFunction<NewsArticle,QueryParser>{
	/*
	 * *This is FlatMap function that maps each query to a newsArticle and returns a
	 * QueryParser list containing the term frequency counts per query. 
	 */
	
	private static final long serialVersionUID = 1L;
	
	Broadcast<List<Query>> queriesBroadcast;
	LongAccumulator totalDocLength;
	
	private TextPreProcessor processor;
	private TermFrequencyCounter frequencyCounter;
	
	
	public QueryParserFlatMap(Broadcast<List<Query>> queriesBroadcast, LongAccumulator totalDocLength) {
		super();
		this.queriesBroadcast = queriesBroadcast;
		this.totalDocLength = totalDocLength;
	}



	@Override
	public Iterator<QueryParser> call(NewsArticle article) throws Exception {
		
		//Initialises objects for pre-processing if there's none in the cache.
		
		if(frequencyCounter == null) {
			frequencyCounter = new TermFrequencyCounter();
		}
		if(processor == null) {
			processor = new TextPreProcessor();
		}
		
		
		//Gets the content of the article as a list
		List<ContentItem> content = article.getContents();
		//An empty to list to store the processed content
		List<ContentItem> contentList = new ArrayList<ContentItem>();
		
		//Gets the title from the article and process
		String title = article.getTitle();
		List<String> processedTitle = processor.process(title);
		//initialise the article lenght by size of the title
		int documentLength = processedTitle.size();
		
		//Pass through the content for the first 5 paragraphs
		 int i = 0;
		//Keeps track of number of paragraphs
		int paragraphCounter = 0;
		while((i < content.size()) && (paragraphCounter<5) ) {
			
			try {
				if((content.get(i).getSubtype().equalsIgnoreCase("paragraph"))&&(content.get(i).getSubtype() != null)){
					
					paragraphCounter++;
					
					List<String> processedContents = processor.process(content.get(i).getContent());
					
					documentLength = documentLength + processedContents.size();
					
					//replace the current paragraph with the processed paragraph
					content.get(i).setContent(processedContents.toString());
					contentList.add(content.get(i));
					
				}
				i++;
				
			} catch (Exception e) {
				break;
			}
			
		}
		article.setContents(contentList);
		
		if(documentLength != 0) {
			
			totalDocLength.add(documentLength);
			
			//Gets the query list from the broadcast variable
			List<Query> queryList = queriesBroadcast.value();
			
			List<QueryParser> queryParserList = new ArrayList<QueryParser>(queryList.size());
			
			for(Query myquery: queryList){
				
				List<String> queryTerms = myquery.getQueryTerms();
				
				short[] queryTermCounts = new short[queryTerms.size()];
				
				int termCounter = 0;
				
				for(String term: queryTerms) {
					
					short numberOfTerms = frequencyCounter.calculateTermFrequency(term, processedTitle);
					
					for(ContentItem item:contentList) {
						
						List<String> contentProcessed = this.processor.process(item.getContent());
						numberOfTerms   += frequencyCounter.calculateTermFrequency(term, contentProcessed);
					}
					
					queryTermCounts[termCounter] = numberOfTerms;
					termCounter++;
					
				}
				
				QueryParser parser = new QueryParser();
				
				parser.setArticle(article);
				parser.setQuery(myquery);
				parser.setTermFrequency(queryTermCounts);
				parser.setDocumentLength(documentLength);
				
				queryParserList.add(parser);
			}
			
			//Return the parsed list of query and article
			return queryParserList.iterator();
			
		}
		//Return an empty list
		else {
			List<QueryParser> queryParserList = new ArrayList<QueryParser>(0);
			return queryParserList.iterator();
		}
		
	}
	
	
}
