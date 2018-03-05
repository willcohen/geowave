package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import com.vividsolutions.jts.geom.Geometry;

public class GeomWithinDistance extends GeomFunction {

	private double radius;

	public GeomWithinDistance(double radius) {
		this.radius = radius;
	}
	
	@Override
	public Boolean call(
			byte[] t1,
			byte[] t2 )
			throws Exception {
		Geometry leftGeom = parseGeom(t1);
		Geometry rightGeom = parseGeom(t2);
		if(leftGeom != null && rightGeom != null) {
			return (leftGeom.distance(rightGeom) <= this.radius);
		}
		return false;
	}
	
	public double getRadius() { return radius; }
}