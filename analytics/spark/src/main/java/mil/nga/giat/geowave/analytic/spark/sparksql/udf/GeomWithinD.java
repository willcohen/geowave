package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import org.apache.spark.sql.api.java.UDF3;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;

public class GeomWithinD implements UDF3<byte[],byte[],Double,Boolean> {

	private final static Logger LOGGER = LoggerFactory.getLogger(GeomFunction.class);
	protected final GeomReader geomReader = new GeomReader();

	protected Geometry parseGeom(
			byte[] geomBinary ) {
		try {
			return geomReader.read(geomBinary);
		}
		catch (Exception e) {
			LOGGER.error(e.getMessage());
		}

		return null;
	}
	
	@Override
	public Boolean call(
			byte[] t1,
			byte[] t2,
			Double radius)
			throws Exception {
		Geometry leftGeom = parseGeom(t1);
		Geometry rightGeom = parseGeom(t2);
		if(leftGeom != null && rightGeom != null) {
			return (leftGeom.distance(rightGeom) <= radius);
		}
		return false;
	}
	
}