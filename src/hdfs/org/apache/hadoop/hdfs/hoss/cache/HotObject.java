package org.apache.hadoop.hdfs.hoss.cache;


public class HotObject implements Comparable<HotObject>{
	/**
	* object name
	*/ 
	private String name;
	/**
	* object hostness
	*/
	private Float hot;
				
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Float getHot() {
		return hot;
	}
	public void setHot(float hot) {
		this.hot = hot;
	}
	public HotObject(float hot, String name){
		this.hot = hot;
		this.name = name;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = name.hashCode() * prime + hot.hashCode();
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		boolean flag = false;
		if(obj instanceof HotObject){
			HotObject ho = (HotObject)obj;
			if(ho.getName().equals(name) && ho.getHot() == hot){
				flag = true;
			}
		}
		return flag;
	}
	@Override
	public int compareTo(HotObject ho) {
		int result = -1;
		result = hot.compareTo(ho.getHot());
		if(result == 0){
			result = name.compareTo(ho.getName());
		}
		return result;
	}
}
