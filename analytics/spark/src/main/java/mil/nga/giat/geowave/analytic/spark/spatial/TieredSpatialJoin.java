package mil.nga.giat.geowave.analytic.spark.spatial;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.kryo.PersistableSerializer;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.SingleTierSubStrategy;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class TieredSpatialJoin implements SpatialJoin {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(TieredSpatialJoin.class);
	
	//Combined matching pairs
	public JavaPairRDD<GeoWaveInputKey, String> joinResults = null;
	
	private List<Tuple2<Byte, JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>>>> leftDataTiers = new ArrayList<>();
	private List<Tuple2<Byte, JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>>>> rightDataTiers = new ArrayList<>();
	
	//Final joined pair RDDs
	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftJoined = null;
	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightJoined = null;

	
	public TieredSpatialJoin() {}
	

	
	@Override
	public void join(
			SparkSession spark,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftRDD,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightRDD,
			GeomFunction predicate,
			NumericIndexStrategy indexStrategy) {
		//Get SparkContext from session
		SparkContext sc = spark.sparkContext();
		
		TieredSFCIndexStrategy tieredStrategy = (TieredSFCIndexStrategy) indexStrategy;
		ClassTag<TieredSFCIndexStrategy> tieredClassTag = scala.reflect.ClassTag$.MODULE$.apply(TieredSFCIndexStrategy.class);
		//Create broadcast variable for indexing strategy
		Broadcast<TieredSFCIndexStrategy> broadcastStrategy = sc.broadcast(tieredStrategy, tieredClassTag );
		
		//Generate Index RDDs for each set of data.
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> leftIndex = this.indexData(leftRDD, broadcastStrategy);
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> rightIndex = this.indexData(rightRDD, broadcastStrategy);
		
		leftIndex.cache();
		rightIndex.cache();
		//This function creates a list of tiers that actually contain data for each set.
		this.collectDataTiers(leftIndex, rightIndex, tieredStrategy);
		
		LOGGER.warn("------------ Beginning Reproject and Join ---------------");
		
		//Iterate through tiers and join each tier containing data from each set.
		for(Tuple2<Byte, JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> t : this.leftDataTiers) {
			Byte leftTierId = t._1();
			
			//Filter left feature set for tier
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> leftTier = t._2();
			leftTier.cache();
			
			for(Tuple2<Byte, JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> t2 : this.rightDataTiers) {
				//Filter the tier from right dataset
				Byte rightTierId = t2._1();
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> rightTier = t2._2();
				
				//We found a tier on the right with geometry to test against.
				//Reproject one of the data sets to the coarser tier
				if(leftTierId > rightTierId) {
					leftTier = this.reprojectToTier(leftTier, rightTierId, broadcastStrategy);
					//caching reprojected tier so it doesn't need to be reevaluated for join
					leftTier.cache();
				} else if (leftTierId < rightTierId) {
					rightTier = this.reprojectToTier(rightTier, leftTierId, broadcastStrategy);
					rightTier.cache();
				}

				//Once we have each tier and index at same resolution then join and compare each of the sets
				JavaPairRDD<GeoWaveInputKey, String> finalTierMatches = this.joinAndCompareTiers(leftTier,rightTier, predicate);
				
				//Combine each tier into a final list of matches for all tiers
				if(this.joinResults == null) {
					this.joinResults = finalTierMatches;
				} else {
					this.joinResults = this.joinResults.union(finalTierMatches);
				}
			}

			leftTier.unpersist();
		}
		
		//cache the resulting keys so we don't have to calculate again for final join.
		this.joinResults.cache();
		//Remove duplicates
		this.joinResults = this.joinResults.reduceByKey((f1,f2) -> f1);
		

		LOGGER.warn("------------ Beginning Final Join ---------------");
		this.leftJoined = this.joinResults.join(leftRDD).mapToPair( t -> new Tuple2<GeoWaveInputKey,SimpleFeature>(t._1(),t._2._2()) );
		this.rightJoined = this.joinResults.join(rightRDD).mapToPair( t -> new Tuple2<GeoWaveInputKey,SimpleFeature>(t._1(),t._2._2()) );
	}
	
	private void collectDataTiers(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> leftIndex,
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> rightIndex,
			TieredSFCIndexStrategy strategy) {
		
		SubStrategy[] tierStrategies = strategy.getSubStrategies();
		int tierCount = tierStrategies.length;
		byte minTierId = (byte) 0;
		byte maxTierId = (byte)(tierCount - 1);
		
		for(int iTier = maxTierId; iTier >= minTierId; iTier--) {
			SingleTierSubStrategy tierStrategy = (SingleTierSubStrategy) tierStrategies[iTier].getIndexStrategy();
			byte tierId = tierStrategy.tier;
			
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> leftTier = this.filterTier(leftIndex, tierId);
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> rightTier = this.filterTier(rightIndex, tierId);
			
			if(!leftTier.isEmpty()) {
				Tuple2<Byte, JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> pair = new Tuple2<>(tierId, leftTier);

				this.leftDataTiers.add(pair);
			}
			
			if(!rightTier.isEmpty()) {
				Tuple2<Byte, JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> pair = new Tuple2<>(tierId, rightTier);

				this.rightDataTiers.add(pair);
			}
			
		}
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> indexData(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> data,
			Broadcast<TieredSFCIndexStrategy> broadcastStrategy)
	{
		//Flat map is used because each pair can potentially yield 1+ output rows within rdd.
		//Instead of storing whole feature on index maybe just output Key + Bounds
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> indexedData = data.flatMapToPair(new PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>,ByteArrayId, Tuple2<GeoWaveInputKey,String>>() {
			@Override
			public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> call(
					Tuple2<GeoWaveInputKey, SimpleFeature> t )
					throws Exception {
				
				//SpatialDimensionalityTypeProvider provider = new SpatialDimensionalityTypeProvider();
				//PrimaryIndex index = provider.createPrimaryIndex();
				//TieredSFCIndexStrategy strategy = (TieredSFCIndexStrategy) index.getIndexStrategy();
				
				//Flattened output array.
				List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> result = new ArrayList<>();


				//Pull feature to index from tuple
				SimpleFeature inputFeature = t._2;
				
				Geometry geom = (Geometry)inputFeature.getDefaultGeometry();
				if(geom == null) {
					return result.iterator();
				}
				String geomString = geom.toText();
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
				List<ByteArrayId> insertIds = broadcastStrategy.value().getInsertionIds(convertedBounds);
				
				//Sometimes the result can span more than one row/cell of a tier
				//When we span more than one row each individual get added as a separate output pair
				for(Iterator<ByteArrayId> iter = insertIds.iterator(); iter.hasNext();) {
					ByteArrayId id = iter.next();
					//Id decomposes to byte array of Tier, Bin, SFC (Hilbert in this case) id)
					//There may be value in decomposing the id and storing tier + sfcIndex as a tuple key of new RDD
					Tuple2<GeoWaveInputKey, String> valuePair = new Tuple2<>(t._1, geomString);
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>(id, valuePair );
					result.add(indexPair);
				}
				
				return result.iterator();
			}
			
		});		
		return indexedData;
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> filterTier(JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> indexRDD, byte tierId) {
		return indexRDD.filter(v1 -> v1._1().getBytes()[0] == tierId);	
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> reprojectToTier(JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> tierIndex, 
			byte targetTierId,
			Broadcast<TieredSFCIndexStrategy> broadcastStrategy) {
		return tierIndex.flatMapToPair(new PairFlatMapFunction<Tuple2<ByteArrayId,Tuple2<GeoWaveInputKey, String>>,ByteArrayId,Tuple2<GeoWaveInputKey, String>>() {

			@Override
			public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> call(
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>> t )
					throws Exception {
				
				List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>> reprojected = new ArrayList<>();
				

				SpatialDimensionalityTypeProvider provider = new SpatialDimensionalityTypeProvider();
				PrimaryIndex index = provider.createPrimaryIndex();
				TieredSFCIndexStrategy strategy = (TieredSFCIndexStrategy) index.getIndexStrategy();

				SubStrategy[] strats = strategy.getSubStrategies();
				
				if(broadcastStrategy.value().tierExists(targetTierId) == false) {
					LOGGER.warn("Tier does not exist in strategy!");
					return reprojected.iterator();
				}
				
				int stratCount = strats.length;
				SingleTierSubStrategy targetStrategy = null;
				for(int i = 0; i < stratCount; i++) {
					SingleTierSubStrategy tierStrategy = (SingleTierSubStrategy) strats[i].getIndexStrategy();
					if(tierStrategy.tier == targetTierId) {
						targetStrategy = tierStrategy;
						break;
					}
				}
				
				//Parse geom from string
				GeomReader reader = new GeomReader();
				Geometry geom = reader.read(t._2._2());
				
				NumericRange xRange = new NumericRange(geom.getEnvelopeInternal().getMinX(), geom.getEnvelopeInternal().getMaxX());
				NumericRange yRange = new NumericRange(geom.getEnvelopeInternal().getMinY(), geom.getEnvelopeInternal().getMaxY());
				NumericData[] boundsRange = {
					xRange,
					yRange	
				};
				
				//Convert the data to how the api expects and index using strategy above
				BasicNumericDataset convertedBounds = new BasicNumericDataset(boundsRange);
				List<ByteArrayId> insertIds = targetStrategy.getInsertionIds(convertedBounds);
				
				//When we span more than one row each individual get added as a separate output pair
				for(Iterator<ByteArrayId> iter = insertIds.iterator(); iter.hasNext();) {
					ByteArrayId id = iter.next();
					//Id decomposes to byte array of Tier, Bin, SFC (Hilbert in this case) id)
					//There may be value in decomposing the id and storing tier + sfcIndex as a tuple key of new RDD
					Tuple2<GeoWaveInputKey, String> valuePair = new Tuple2<>(t._2._1, t._2._2);
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, String>>(id, valuePair);
					reprojected.add(indexPair);
				}
				
				return reprojected.iterator();
			}
			
		});
	}
	
	private JavaPairRDD<GeoWaveInputKey, String> joinAndCompareTiers(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> leftTier,
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, String>> rightTier,
			GeomFunction predicate) {
		//Cogroup looks at each RDD and grab keys that are the same in this case ByteArrayId
		
		JavaPairRDD<GeoWaveInputKey, String> finalMatches = null;
		JavaPairRDD<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, String>>, Iterable<Tuple2<GeoWaveInputKey, String>>>> joinedTiers = leftTier.cogroup(rightTier);
		//We need to go through the pairs and test each feature against each other
		//End with a combined RDD for that tier.
		finalMatches = joinedTiers.flatMapToPair(new PairFlatMapFunction<Tuple2<ByteArrayId,Tuple2<Iterable<Tuple2<GeoWaveInputKey, String>>,Iterable<Tuple2<GeoWaveInputKey, String>>>>, GeoWaveInputKey, String>() {

			@Override
			public Iterator<Tuple2<GeoWaveInputKey, String>> call(
					Tuple2<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, String>>, Iterable<Tuple2<GeoWaveInputKey, String>>>> t )
					throws Exception {
				List<Tuple2<GeoWaveInputKey, String>> resultPairs = new ArrayList<>();
				
				Iterable<Tuple2<GeoWaveInputKey, String>> leftFeatures = t._2._1();
				Iterable<Tuple2<GeoWaveInputKey, String>> rightFeatures = t._2._2();
				
				//Compare each filtered set against one another and add feature pairs that
				for (Tuple2<GeoWaveInputKey, String> leftTuple : leftFeatures) {
					
					for (Tuple2<GeoWaveInputKey, String> rightTuple : rightFeatures ) {
						
						if(predicate.call(leftTuple._2, rightTuple._2)) {
							Tuple2<GeoWaveInputKey,String> leftPair = new Tuple2<>(leftTuple._1,leftTuple._2);
							Tuple2<GeoWaveInputKey,String> rightPair = new Tuple2<>(rightTuple._1,rightTuple._2);
							resultPairs.add(leftPair);
							resultPairs.add(rightPair);
						}
						
					}
					
				}
				
				return resultPairs.iterator();
			}
			
		});

		//Remove duplicates from previous step
		//finalMatches = finalMatches.reduceByKey((s1,s2) -> s1);		
		
		return finalMatches;
	}

}