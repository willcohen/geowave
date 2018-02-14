package mil.nga.giat.geowave.analytic.spark;

import org.apache.spark.serializer.KryoRegistrator;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.opengis.feature.simple.SimpleFeature;

import com.esotericsoftware.kryo.Kryo;

import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeature;
import mil.nga.giat.geowave.analytic.kryo.FeatureSerializer;
import mil.nga.giat.geowave.analytic.kryo.PersistableSerializer;
import mil.nga.giat.geowave.analytic.spark.spatial.TieredSpatialJoin;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.sfc.hilbert.HilbertSFC;
import mil.nga.giat.geowave.core.index.sfc.tiered.SingleTierSubStrategy;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;

public class GeoWaveRegistrator implements
		KryoRegistrator
{
	public void registerClasses(
			Kryo kryo ) {
		// Use existing FeatureSerializer code to serialize SimpleFeature
		// classes
		FeatureSerializer simpleSerializer = new FeatureSerializer();
		PersistableSerializer persistSerializer = new PersistableSerializer();

		kryo.register(
				SimpleFeature.class,
				simpleSerializer);
		kryo.register(
				SimpleFeatureImpl.class,
				simpleSerializer);
		kryo.register(
				AvroSimpleFeature.class,
				simpleSerializer);
		
		kryo.register(Persistable.class, persistSerializer);
		kryo.register(TieredSFCIndexStrategy.class, persistSerializer);
		kryo.register(SingleTierSubStrategy.class, persistSerializer);
		kryo.register(HilbertSFC.class, persistSerializer);
	}
}