package es.joseluisllorente.kafka.advanced.beans;

public class Topic {
	private String name;
	private int numPartitions;
	private int repFactor;
	
	public Topic(String name) {
		super();
		this.name = name;
	}

	public Topic(String name, int numPartitions, int repFactor) {
		super();
		this.name = name;
		this.numPartitions = numPartitions;
		this.repFactor = repFactor;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}

	public int getRepFactor() {
		return repFactor;
	}

	public void setRepFactor(int repFactor) {
		this.repFactor = repFactor;
	}

	@Override
	public String toString() {
		return "Topic [name=" + name + ", numPartitions=" + numPartitions + ", repFactor=" + repFactor + "]";
	}
	
	
}
