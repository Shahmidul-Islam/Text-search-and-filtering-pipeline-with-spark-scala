package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryParser;

public class QueryFilter implements MapFunction<QueryParser, Query>{

	/**
	 * Filters a query from the query Parser object
	 * Will serve as a key in the mapGroup functions
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Query call(QueryParser value) throws Exception {
		return value.getQuery();
	}


}
