package mil.nga.giat.geowave.analytic.spark.spatial;

import java.io.Serializable;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class CommonIndexType implements Serializable {
	private byte[] insertionId;
	private GeoWaveInputKey dataId;
	private Geometry geom;
	
	public CommonIndexType() {}
	
	public CommonIndexType(byte[] indexId, GeoWaveInputKey inputKey, Geometry geom) {
		this.setInsertionId(
				indexId);
		this.setDataId(
				inputKey);
		this.setGeom(
				geom);
	}


	public GeoWaveInputKey getDataId() {
		return dataId;
	}

	public void setDataId(
			GeoWaveInputKey dataId ) {
		this.dataId = dataId;
	}

	public Geometry getGeom() {
		return geom;
	}

	public void setGeom(
			Geometry geom ) {
		this.geom = geom;
	}

	public byte[] getInsertionId() {
		return insertionId;
	}

	public void setInsertionId(
			byte[] insertionId ) {
		this.insertionId = insertionId;
	}
}