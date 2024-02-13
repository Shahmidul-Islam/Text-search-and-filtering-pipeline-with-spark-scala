package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class QueryParser implements Serializable{

	/**
	 * Structure for storing results of the flatmap on query and NewsArticle
	 */
	private static final long serialVersionUID = 1L;
	
	private NewsArticle article;
	private Query query;
	private int documentLength;
	short[] termFrequency;
	public QueryParser(NewsArticle article, Query query, int documentLength, short[] termFrequency) {
		super();
		this.article = article;
		this.query = query;
		this.documentLength = documentLength;
		this.termFrequency = termFrequency;
	}
	public QueryParser() {
		
	}
	public NewsArticle getArticle() {
		return article;
	}
	public void setArticle(NewsArticle article) {
		this.article = article;
	}
	public Query getQuery() {
		return query;
	}
	public void setQuery(Query query) {
		this.query = query;
	}
	public int getDocumentLength() {
		return documentLength;
	}
	public void setDocumentLength(int documentLength) {
		this.documentLength = documentLength;
	}
	public short[] getTermFrequency() {
		return termFrequency;
	}
	public void setTermFrequency(short[] termFrequency) {
		this.termFrequency = termFrequency;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	
	
	
	
}
