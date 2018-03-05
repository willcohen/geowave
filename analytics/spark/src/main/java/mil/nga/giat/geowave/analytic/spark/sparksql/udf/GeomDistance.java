package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import org.apache.spark.sql.api.java.UDF2;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;

public class GeomDistance implements UDF2<byte[], byte[], Double> {

	private GeomReader reader = new GeomReader();
	
	@Override
	public Double call(
			byte[] t1,
			byte[] t2 )
			throws Exception {
		Geometry leftGeom = reader.read(t1);
		Geometry rightGeom = reader.read(t2);
		if(leftGeom != null && rightGeom != null)
			return leftGeom.distance(rightGeom);
		
		return Double.MAX_VALUE;
	}
	
}