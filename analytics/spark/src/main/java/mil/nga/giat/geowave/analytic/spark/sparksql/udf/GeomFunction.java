package mil.nga.giat.geowave.analytic.spark.sparksql.udf;

import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;

@SuppressFBWarnings
public abstract class GeomFunction implements
		UDF2<Geometry, Geometry, Boolean>
{ }
