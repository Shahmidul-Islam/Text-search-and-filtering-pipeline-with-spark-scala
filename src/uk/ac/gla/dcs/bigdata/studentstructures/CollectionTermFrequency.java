package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

public class CollectionTermFrequency implements Serializable{

	/**
	 * Structure for storing total term frequency
	 */
	private static final long serialVersionUID = 1L;
	
	private int[] freq;
	
	public CollectionTermFrequency() {
	}

	public CollectionTermFrequency(int[] freq) {
		super();
		this.freq = freq;
	}

	public int[] getFreq() {
		return freq;
	}

	public void setFreq(int[] freq) {
		this.freq = freq;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	
	

}
