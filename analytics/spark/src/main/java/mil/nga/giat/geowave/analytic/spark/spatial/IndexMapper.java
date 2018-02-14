package mil.nga.giat.geowave.analytic.spark.spatial;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;

public abstract class IndexMapper implements
FlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>, CommonIndexType> {
	
	protected StructType schema = null;
	
	public IndexMapper() {
		
	}
	
	public StructType getSchema() {
		return this.schema;
	}
	
}