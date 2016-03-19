package org.apache.hadoop.hdfs.hoss.cache;

import org.apache.hadoop.hdfs.hoss.db.PathPosition;
/**
 * HOSS object meta 
 * 
 * /
public class Metadata {
	private String objName;
	private long objId;
	private PathPosition pathPosition;
	
	public Metadata(long objId, PathPosition pathPosition) {
		super();
		objName = "";
		this.objId = objId;
		this.pathPosition = pathPosition;
	}
	
	public Metadata(long objId, PathPosition pathPosition, String objName) {
		super();
		this.objName = objName;
		this.objId = objId;
		this.pathPosition = pathPosition;
	}
	
	public String getObjName() {
		return objName;
	}

	public void setObjName(String objName) {
		this.objName = objName;
	}
	
	public long getObjId() {
		return objId;
	}
	
	public void setObjId(long objId) {
		this.objId = objId;
	}
	
	public PathPosition getPathPosition() {
		return pathPosition;
	}
	public void setPathPosition(PathPosition pathPosition) {
		this.pathPosition = pathPosition;
	}
	
	@Override
	public int hashCode() {
		return (int) (pathPosition.hashCode() + 31 * objId);
	}
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Metadata) {
			Metadata pp = (Metadata) obj;
			return pathPosition.equals(pp.getPathPosition()) && objId == pp.getObjId();
		}
		return false;
	}
	@Override
	public String toString() {
		return "object id: " + objId + "   " + pathPosition;
	}

}
