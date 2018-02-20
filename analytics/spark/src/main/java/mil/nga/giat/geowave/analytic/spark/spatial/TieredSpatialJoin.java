package mil.nga.giat.geowave.analytic.spark.spatial;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomWriter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
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
	private JavaPairRDD<GeoWaveInputKey, byte[]> combinedResults = null;
	
	private List<Byte> leftDataTiers = new ArrayList<>();
	private List<Byte> rightDataTiers = new ArrayList<>();
	
	//Final joined pair RDDs
	private JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftJoined = null;
	private JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightJoined = null;

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
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> leftIndex = this.indexData(leftRDD, broadcastStrategy);
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> rightIndex = this.indexData(rightRDD, broadcastStrategy);

		JavaRDD<ByteArrayId> leftKeys = leftIndex.keys();
		JavaRDD<ByteArrayId> rightKeys = rightIndex.keys();
		
		JavaPairRDD<Byte, ByteArrayId> tieredLeftKeys = leftKeys.keyBy(id -> new Byte(id.getBytes()[0]));
		JavaPairRDD<Byte, ByteArrayId> tieredRightKeys = rightKeys.keyBy(id -> new Byte(id.getBytes()[0]));
		
		tieredLeftKeys = tieredLeftKeys.reduceByKey((id1, id2) -> id1);
		tieredRightKeys = tieredRightKeys.reduceByKey((id1, id2) -> id1);
		
		tieredLeftKeys.cache();
		tieredRightKeys.cache();
		
		this.collectDataTiers(tieredLeftKeys, tieredRightKeys, tieredStrategy);
		
		tieredLeftKeys.unpersist();
		tieredRightKeys.unpersist();
		
		for(Byte leftTierId : leftDataTiers) {
			
			//Filter left feature set for tier
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> leftTier = this.filterTier(leftIndex, leftTierId);
			
			
			for(Byte rightTierId : rightDataTiers) {
				//Filter the tier from right dataset
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> rightTier = this.filterTier(rightIndex, rightTierId);
				
				//We found a tier on the right with geometry to test against.
				//Reproject one of the data sets to the coarser tier
				JavaRDD<ByteArrayId> reprojectKeys = null;
				if(leftTierId > rightTierId) {
					leftTier = this.reprojectToTier(leftTier, rightTierId, broadcastStrategy);
					//caching reprojected tier keys so they don't need to be reevaluated for join
					reprojectKeys = leftTier.keys();
					reprojectKeys.cache();
				} else if (leftTierId < rightTierId) {
					rightTier = this.reprojectToTier(rightTier, leftTierId, broadcastStrategy);
					reprojectKeys = rightTier.keys();
					reprojectKeys.cache();
				}

				//Once we have each tier and index at same resolution then join and compare each of the sets
				JavaPairRDD<GeoWaveInputKey, byte[]> finalTierMatches = this.joinAndCompareTiers(leftTier,rightTier, predicate);
				
				//Combine each tier into a final list of matches for all tiers
				this.addMatches(finalTierMatches);
				
				reprojectKeys.unpersist();
					
			}
			
			//leftTier.unpersist();

		}
		//Remove duplicates
		this.combinedResults = this.combinedResults.reduceByKey((f1,f2) -> f1);
		
		//Cache results
		//this.combinedResults.cache();
		
		//Join against original dataset to give final joined rdds on each side
		this.setLeftJoined(
				this.combinedResults.join(leftRDD).mapToPair( t -> new Tuple2<GeoWaveInputKey,SimpleFeature>(t._1(),t._2._2()) ));
		this.setRightJoined(
				this.combinedResults.join(rightRDD).mapToPair( t -> new Tuple2<GeoWaveInputKey,SimpleFeature>(t._1(),t._2._2()) ));
		
		//long leftCount = this.leftJoined.count();
		//long rightCount = this.rightJoined.count();
		//LOGGER.warn("Final Counts: " + leftCount + " Left, " + rightCount + " Right");
	}
	
	private void collectDataTiers(
			JavaPairRDD<Byte, ByteArrayId> tieredLeftKeys,
			JavaPairRDD<Byte, ByteArrayId> tieredRightKeys,
			TieredSFCIndexStrategy tieredStrategy ) {
		
	
		SubStrategy[] tierStrategies = tieredStrategy.getSubStrategies();
		int tierCount = tierStrategies.length;
		byte minTierId = (byte) 0;
		byte maxTierId = (byte)(tierCount - 1);
		
		for(int iTier = maxTierId; iTier >= minTierId; iTier--) {
			SingleTierSubStrategy tierStrategy = (SingleTierSubStrategy) tierStrategies[iTier].getIndexStrategy();
			byte tierId = tierStrategy.tier;
			
			JavaPairRDD<Byte, ByteArrayId> leftTier = tieredLeftKeys.filter(t -> t._1 == tierId);
			JavaPairRDD<Byte, ByteArrayId> rightTier = tieredRightKeys.filter(t -> t._1 == tierId);
			if(!leftTier.isEmpty()) {
				this.leftDataTiers.add(tierId);
			}
			
			if(!rightTier.isEmpty()) {
				this.rightDataTiers.add(tierId);
			}
		}

	}

	private void addMatches(JavaPairRDD<GeoWaveInputKey, byte[]> matches) {
		if(this.combinedResults == null) {
			this.combinedResults = matches;
		} else {
			this.combinedResults = this.combinedResults.union(matches);
		}
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> indexData(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> data,
			Broadcast<TieredSFCIndexStrategy> broadcastStrategy)
	{
		//Flat map is used because each pair can potentially yield 1+ output rows within rdd.
		//Instead of storing whole feature on index maybe just output Key + Bounds
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> indexedData = data.flatMapToPair(new PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>,ByteArrayId, Tuple2<GeoWaveInputKey,byte[]>>() {
			@Override
			public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>>> call(
					Tuple2<GeoWaveInputKey, SimpleFeature> t )
					throws Exception {
				
				//Flattened output array.
				List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>>> result = new ArrayList<>();


				//Pull feature to index from tuple
				SimpleFeature inputFeature = t._2;
				
				Geometry geom = (Geometry)inputFeature.getDefaultGeometry();
				if(geom == null) {
					return result.iterator();
				}
				GeomWriter wkbWriter = new GeomWriter();
				byte[] geomBytes = wkbWriter.write(geom);
				
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
					Tuple2<GeoWaveInputKey, byte[]> valuePair = new Tuple2<>(t._1, geomBytes);
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>>(id, valuePair );
					result.add(indexPair);
				}
				
				return result.iterator();
			}
			
		});		
		return indexedData;
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> filterTier(JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> indexRDD, byte tierId) {
		return indexRDD.filter(v1 -> v1._1().getBytes()[0] == tierId);	
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> reprojectToTier(JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> tierIndex, 
			byte targetTierId,
			Broadcast<TieredSFCIndexStrategy> broadcastStrategy) {
		return tierIndex.flatMapToPair(new PairFlatMapFunction<Tuple2<ByteArrayId,Tuple2<GeoWaveInputKey, byte[]>>,ByteArrayId,Tuple2<GeoWaveInputKey, byte[]>>() {

			@Override
			public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>>> call(
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> t )
					throws Exception {
				
				List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>>> reprojected = new ArrayList<>();
				

				SubStrategy[] strats = broadcastStrategy.value().getSubStrategies();
				
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
					Tuple2<GeoWaveInputKey, byte[]> valuePair = new Tuple2<>(t._2._1, t._2._2);
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>>(id, valuePair);
					reprojected.add(indexPair);
				}
				
				return reprojected.iterator();
			}
			
		});
	}
	
	private JavaPairRDD<GeoWaveInputKey, byte[]> joinAndCompareTiers(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> leftTier,
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> rightTier,
			GeomFunction predicate) {
		//Cogroup looks at each RDD and grab keys that are the same in this case ByteArrayId
		JavaPairRDD<GeoWaveInputKey, byte[]> finalMatches = null;
		JavaPairRDD<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, byte[]>>, Iterable<Tuple2<GeoWaveInputKey, byte[]>>>> joinedTiers = leftTier.cogroup(rightTier);
		//We need to go through the pairs and test each feature against each other
		//End with a combined RDD for that tier.
		finalMatches = joinedTiers.flatMapToPair(new PairFlatMapFunction<Tuple2<ByteArrayId,Tuple2<Iterable<Tuple2<GeoWaveInputKey, byte[]>>,Iterable<Tuple2<GeoWaveInputKey, byte[]>>>>, GeoWaveInputKey, byte[]>() {

			@Override
			public Iterator<Tuple2<GeoWaveInputKey, byte[]>> call(
					Tuple2<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, byte[]>>, Iterable<Tuple2<GeoWaveInputKey, byte[]>>>> t )
					throws Exception {
				List<Tuple2<GeoWaveInputKey, byte[]>> resultPairs = new ArrayList<>();
				
				Iterable<Tuple2<GeoWaveInputKey, byte[]>> leftFeatures = t._2._1();
				Iterable<Tuple2<GeoWaveInputKey, byte[]>> rightFeatures = t._2._2();
				
				//Compare each filtered set against one another and add feature pairs that
				for (Tuple2<GeoWaveInputKey, byte[]> leftTuple : leftFeatures) {
					
					for (Tuple2<GeoWaveInputKey, byte[]> rightTuple : rightFeatures ) {
						
						if(predicate.call(leftTuple._2, rightTuple._2)) {
							Tuple2<GeoWaveInputKey,byte[]> leftPair = new Tuple2<>(leftTuple._1,leftTuple._2);
							Tuple2<GeoWaveInputKey,byte[]> rightPair = new Tuple2<>(rightTuple._1,rightTuple._2);
							resultPairs.add(leftPair);
							resultPairs.add(rightPair);
						}
						
					}
					
				}
				
				return resultPairs.iterator();
			}
			
		});
		
		return finalMatches;
	}

	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getLeftJoined() {
		return leftJoined;
	}

	public void setLeftJoined(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftJoined ) {
		this.leftJoined = leftJoined;
	}

	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getRightJoined() {
		return rightJoined;
	}

	public void setRightJoined(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightJoined ) {
		this.rightJoined = rightJoined;
	}
	
	public long getCount(boolean left) {
		return (left) ? leftJoined.count() : rightJoined.count();
	}

}