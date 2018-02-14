package mil.nga.giat.geowave.analytic.spark.spatial;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class IndexedDataset {
	private final SparkSession sparkSession;
	private JavaRDD<CommonIndexType> indexedRDD = null;
	private Dataset<CommonIndexType> dataFrame = null;
	private StructType schema = null;
	private IndexMapper mapper;
	
	public IndexedDataset(SparkSession session, IndexMapper mapFunc ) {
		this.sparkSession = session;
		this.mapper = mapFunc;
	}
	
	public Dataset<CommonIndexType> getDataFrame(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD ) {

		if(indexedRDD == null) {
			indexedRDD = pairRDD.flatMap(mapper);
		}
		
		if(dataFrame == null) {
			dataFrame = sparkSession.createDataset(indexedRDD.rdd(), Encoders.bean(CommonIndexType.class));
			schema = dataFrame.schema();
		}
		return dataFrame;
	}
	
	public StructType getSchema() {
		return schema;
	}
}