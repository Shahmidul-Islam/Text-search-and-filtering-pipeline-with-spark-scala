package uk.ac.gla.dcs.bigdata.studentfunctions;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryParser;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.CollectionTermFrequency;

import java.util.Iterator;

import org.apache.spark.api.java.function.MapGroupsFunction;

import scala.Tuple2;

public class CollectionFrequency implements MapGroupsFunction<Query,QueryParser,Tuple2<Query,CollectionTermFrequency>>{

	/**
	 * This function calculates the total term frequency in the corpus when given a query and QueryParser object.
	 * It returns a tuple2 object containing the query and collection term frequency array
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Query, CollectionTermFrequency> call(Query query, Iterator<QueryParser> parser) throws Exception {
		
		short[] queryTermCounts = query.getQueryTermCounts();
		//Declare output array of type int to return the collection frequency
		int[] collectionFrequency = new int[queryTermCounts.length];
		
		CollectionTermFrequency freq = new CollectionTermFrequency();
		
		while(parser.hasNext()) {
			QueryParser qParser = parser.next();
			
			for (int i=0; i < qParser.getTermFrequency().length;i++) {
				
				collectionFrequency[i] += qParser.getTermFrequency()[i];
				
			}
		}
		
		freq.setFreq(collectionFrequency);
		return new Tuple2<Query,CollectionTermFrequency>(query,freq);
	}

}
