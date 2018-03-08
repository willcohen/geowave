package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import com.vividsolutions.jts.geom.Geometry;

public class GeomTouches extends
		GeomFunction
{
	@Override
	public Boolean call(
			Geometry geom1,
			Geometry geom2 )
			throws Exception {

			return geom1.touches(geom2);
	}
}
