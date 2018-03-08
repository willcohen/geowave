package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import com.vividsolutions.jts.geom.Geometry;

public class GeomWithinDistance extends GeomFunction {

	private double radius;

	public GeomWithinDistance(double radius) {
		this.radius = radius;
	}
	
	@Override
	public Boolean call(
			Geometry geom1,
			Geometry geom2 )
			throws Exception {

			return geom1.distance(geom2) <= radius;
	}
	
	public double getRadius() { return radius; }
}