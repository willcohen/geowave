package mil.nga.giat.geowave.analytic.spark.spatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomWriter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;



public class TieredIndexMapper extends IndexMapper
{

	public TieredIndexMapper() {
		super();
		/*StructField[] fields = {
			new StructField("tier", DataTypes.ByteType, false, Metadata.empty()),
			new StructField("id", DataTypes.BinaryType, false, Metadata.empty()),
			new StructField("geom", DataTypes.StringType, false, Metadata.empty()),
			new StructField("gwKey", DataTypes.BinaryType, false, Metadata.empty())
		};
		this.schema = new StructType(fields);*/
	}

	@Override
	public Iterator<CommonIndexType> call(
			Tuple2<GeoWaveInputKey, SimpleFeature> t )
			throws Exception {
		SpatialDimensionalityTypeProvider provider = new SpatialDimensionalityTypeProvider();
		PrimaryIndex index = provider.createPrimaryIndex();
		TieredSFCIndexStrategy strategy = (TieredSFCIndexStrategy) index.getIndexStrategy();
		
		//Flattened output array.
		List<CommonIndexType> result = new ArrayList<>();


		//Pull feature to index from tuple
		SimpleFeature inputFeature = t._2;
		
		Geometry geom = (Geometry)inputFeature.getDefaultGeometry();
		if(geom == null) {
			return result.iterator();
		}
		GeomWriter wkbWriter = new GeomWriter();
		byte[] geomString = wkbWriter.write(geom);
		//Extract bounding box from input feature
		BoundingBox bounds = inputFeature.getBounds();
		NumericRange xRange = new NumericRange(bounds.getMinX(), bounds.getMaxX());
		NumericRange yRange = new NumericRange(bounds.getMinY(), bounds.getMaxY());
		
		if(bounds.isEmpty()) {
			Envelope internalEnvelope = geom.getEnvelopeInternal();
			xRange = new NumericRange(internalEnvelope.getMinX(), internalEnvelope.getMaxX());
			yRange = new NumericRange(internalEnvelope.getMinY(), internalEnvelope.getMaxY());
		
		}
		NumericData[] boundsRange = {
			xRange,
			yRange	
		};
		
		//Convert the data to how the api expects and index using strategy above
		BasicNumericDataset convertedBounds = new BasicNumericDataset(boundsRange);
		List<ByteArrayId> insertIds = strategy.getInsertionIds(convertedBounds);
		
		//Sometimes the result can span more than one row/cell of a tier
		//When we span more than one row each individual get added as a separate output pair
		for(Iterator<ByteArrayId> iter = insertIds.iterator(); iter.hasNext();) {
			ByteArrayId id = iter.next();
			//Id decomposes to byte array of Tier, Bin, SFC (Hilbert in this case) id)
			//There may be value in decomposing the id and storing tier + sfcIndex as a tuple key of new RDD
			//byte[] fullId = id.getBytes();
			//byte tier = fullId[0];

			//Object[] fields = new Serializable[] { tier, id.getBytes(), geomString, t._1().getDataId().getBytes() };
			result.add(new CommonIndexType(id.getBytes(), t._1(), geomString) );
		}
		
		return result.iterator();
	
	}
	
}