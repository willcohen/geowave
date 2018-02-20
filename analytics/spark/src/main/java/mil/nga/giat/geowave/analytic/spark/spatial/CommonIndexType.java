package mil.nga.giat.geowave.analytic.spark.spatial;

import java.io.Serializable;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;

public class CommonIndexType implements Serializable {
	private byte[] insertionId;
	private byte[] adapterId;
	private byte[] dataId;
	private byte[] geom;
	
	public CommonIndexType() {}
	
	public CommonIndexType(byte[] indexId, GeoWaveInputKey inputKey, byte[] geom) {
		this.setInsertionId(
				indexId);
		this.setDataId(
				inputKey);
		this.setAdapterId(
				inputKey);
		this.setGeom(
				geom);
	}
	
	public CommonIndexType(byte[] indexId, byte[] adapterId, byte[] dataId , byte[] geom) {
		this.setInsertionId(
				indexId);
		this.setDataId(
				dataId);
		this.setAdapterId(
				adapterId);
		this.setGeom(
				geom);
	}

	public byte[] getDataId() {
		return dataId;
	}

	public void setDataId(
			byte[] dataId ) {
		this.dataId = dataId;
	}
	
	public void setDataId(
			GeoWaveInputKey inputKey ) {
		this.dataId = inputKey.getDataId().getBytes();
	}

	public byte[] getGeom() {
		return geom;
	}

	public void setGeom(
			byte[] geom ) {
		this.geom = geom;
	}

	public byte[] getInsertionId() {
		return insertionId;
	}

	public void setInsertionId(
			byte[] insertionId ) {
		this.insertionId = insertionId;
	}

	public byte[] getAdapterId() {
		return adapterId;
	}

	public void setAdapterId(
			byte[] adapterId ) {
		this.adapterId = adapterId;
	}
	
	public void setAdapterId(
			GeoWaveInputKey inputKey ) {
		this.adapterId = inputKey.getAdapterId().getBytes();
	}
}