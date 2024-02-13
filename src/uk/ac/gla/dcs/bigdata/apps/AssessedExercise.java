package uk.ac.gla.dcs.bigdata.apps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.CollectionFrequency;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentScorer;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryFilter;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryParserFlatMap;
import uk.ac.gla.dcs.bigdata.studentstructures.CollectionTermFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryParser;

import scala.Tuple2;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise{

	
	public static void main(String[] args) {
		long startime=System.currentTimeMillis();
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		/*
		try {
			rankDocuments(spark, queryFile, newsFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
		
		
		// Close the spark session
		spark.close();
		
		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out!=null) resultsDIR = out;
		

		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			//Printing for testing purposes
			for(int i=0;i<results.size();i++) {
				System.out.println(results.get(i));
			}
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		

		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath()+"/SPARK.DONE")));
			writer.write(String.valueOf(System.currentTimeMillis()));
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long stoptime=System.currentTimeMillis();
		System.out.println(stoptime-startime);
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		//This broadcasts a list of queries to the flatmapfunction
		List<Query> currentQueries= queries.collectAsList();
		Broadcast<List<Query>> queriesBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(currentQueries);
		
		//This creates an accumulator to keep track of document lenght across the entire corpus.
		//The variable is static across all instances
		LongAccumulator totalDocLength = spark.sparkContext().longAccumulator();
		
		
		Dataset<QueryParser> queryParser = news.flatMap(new QueryParserFlatMap(queriesBroadcast,totalDocLength),Encoders.bean(QueryParser.class));
		
		List<QueryParser> parsedList = queryParser.collectAsList();
		
		/*
		 * For testing purposes
		for(int i =0; i < 10; i++) {
			QueryParser list = articleList.get(i);
			//System.out.print(list.getQuery().getOriginalQuery());
			System.out.println("\n");

			short[] tf = list.getTermFrequency();
			String termFrequency = Arrays.toString(tf);
			System.out.print(termFrequency);
			
			//System.out.println(list.getArticle().getArticle_url());
		}*/
		
		//Preparing the various variables for DPHScoring
		/*
		 *  termFrequencyInCurrentDocument // The number of times the query appears in the document: Stored in the QueryParser
		 *  totalTermFrequencyInCorpus // the number of times the query appears in all documents: *****Calculated using a MapGroup of Query and QueryParser
		 *  currentDocumentLength // the length of the current document (number of terms in the document): Stored in QueryParser
		 *  averageDocumentLengthInCorpus // the average length across all documents: Calculated below using the accumulated variable
		 *  totalDocsInCorpus // the number of documents in the corpus: Calculated using length(QueryParser/query)
		 */
		
		long totalDocsInCorpus = parsedList.size()/currentQueries.size();
		
		double averageDocumentLengthInCorpus = (totalDocLength.value().doubleValue())/ (totalDocsInCorpus);
		
		Broadcast<Double> averageDocumentLengthInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);
		Broadcast<Long> totalDocsInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpus);
		
		//A query filter is obtained for the mapGroup functions
		KeyValueGroupedDataset<Query,QueryParser> queryFilter = queryParser.groupByKey(new QueryFilter(), Encoders.bean(Query.class));
		
		//Applies the mapGroup to obtain the collection frequency for each term in a given query
		Dataset<Tuple2<Query,CollectionTermFrequency>> collectionFrequency = queryFilter.mapGroups(new CollectionFrequency(), Encoders.tuple(Encoders.bean(Query.class),Encoders.bean(CollectionTermFrequency.class)));
		
		//Collects the collection frequency tuple as a list of type tuple2(Query and corresponding collection term frequencies)
		List<Tuple2<Query,CollectionTermFrequency>> collectionFreqList = collectionFrequency.collectAsList();
		
		//Broadcasts the collection frequency list into the Document Scorer MapGroups function for scoring
		Broadcast<List<Tuple2<Query,CollectionTermFrequency>>> collectionFreqBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(collectionFreqList);
		
		Dataset<DocumentRanking> rank = queryFilter.mapGroups(new DocumentScorer(averageDocumentLengthInCorpusBroadcast,totalDocsInCorpusBroadcast,collectionFreqBroadcast), Encoders.bean(DocumentRanking.class));
		
		//Return ranked list
		List<DocumentRanking> rankedList = rank.collectAsList();
		
		
		return rankedList;
		

	}
	
	
}
