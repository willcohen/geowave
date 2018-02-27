package mil.nga.giat.geowave.analytic.spark.spatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomWriter;
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
	private JavaPairRDD<GeoWaveInputKey, ByteArrayId> combinedResults = null;
	
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
		
		JavaRDD<Byte> reducedLeftTiers = tieredLeftKeys.keys();
		JavaRDD<Byte> reducedRightTiers = tieredRightKeys.keys();
		
		reducedRightTiers.cache();
		reducedLeftTiers.cache();
		//This reduces the number of jobs to 2 to figure out what tiers have data
		List<Byte> leftTiers = reducedLeftTiers.collect();
		List<Byte> rightTiers = reducedRightTiers.collect();
		
		//this.collectDataTiers(tieredLeftKeys, tieredRightKeys, tieredStrategy);
		
		reducedRightTiers.unpersist();
		reducedLeftTiers.unpersist();
		tieredLeftKeys.unpersist();
		tieredRightKeys.unpersist();
		
		
		Map<Byte, HashSet<Byte>> rightReprojectMap = new HashMap<Byte, HashSet<Byte>>();
		Map<Byte, Byte[]> leftReprojectMap = new HashMap<Byte, Byte[]>();
		for(Byte tierLeft : leftTiers) {
			ArrayList<Byte> higherTiers = new ArrayList<Byte>();
			for(Byte tierRight : rightTiers) {
				if( tierRight > tierLeft ) {
					higherTiers.add(tierRight);
				} else if( tierRight < tierLeft) {
					HashSet<Byte> set = rightReprojectMap.get(tierRight);
					if(set == null) {
						set = new HashSet<Byte>();
						rightReprojectMap.put(tierRight, set);
					}
					set.add(tierLeft);
				}
			}
			
			if(!higherTiers.isEmpty()) {
				Byte[] higherArray = higherTiers.toArray(new Byte[higherTiers.size()]);
				leftReprojectMap.put(tierLeft, higherArray);
			}
		}
		
		
		for(Byte leftTierId : leftTiers) {
			
			//Filter left feature set for tier
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> leftTier = this.filterTier(leftIndex, leftTierId);
			
			
			Byte[] rightHigherTiers = leftReprojectMap.get(leftTierId);
			
			if (rightHigherTiers != null && rightHigherTiers.length > 0) {
				//Filter and reproject right tiers to coarser left tier
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> higherRightTiers = filterTiers(rightIndex, rightHigherTiers);
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> reprojected = reprojectToTier(higherRightTiers, leftTierId, broadcastStrategy);
				JavaPairRDD<GeoWaveInputKey, ByteArrayId> finalTierMatches = this.joinAndCompareTiers(leftTier, reprojected, predicate);
				
				this.addMatches(finalTierMatches);
			}
			
			if(rightTiers.contains(leftTierId)) {
				//Filter and join and same level
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> rightTier = this.filterTier(rightIndex, leftTierId);
				JavaPairRDD<GeoWaveInputKey, ByteArrayId> finalTierMatches = this.joinAndCompareTiers(leftTier, rightTier, predicate);
				
				this.addMatches(finalTierMatches);
			}

		}
		
		for(Byte rightTierId : rightTiers) {
			//Filter left feature set for tier
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> rightTier = this.filterTier(rightIndex, rightTierId);
			
			HashSet<Byte> higherLeftTiers = rightReprojectMap.get(rightTierId);
			
			if(higherLeftTiers != null && !higherLeftTiers.isEmpty()) {
				Byte[] tiers = higherLeftTiers.toArray(new Byte[higherLeftTiers.size()]);
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> filteredLeftTiers = filterTiers(leftIndex, tiers);
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> reprojected = reprojectToTier(filteredLeftTiers, rightTierId, broadcastStrategy);
				JavaPairRDD<GeoWaveInputKey, ByteArrayId> finalTierMatches = this.joinAndCompareTiers(reprojected, rightTier, predicate);
				
				this.addMatches(finalTierMatches);
			}
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

	private void addMatches(JavaPairRDD<GeoWaveInputKey, ByteArrayId> finalTierMatches) {
		if(this.combinedResults == null) {
			this.combinedResults = finalTierMatches;
		} else {
			this.combinedResults = this.combinedResults.union(finalTierMatches);
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
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> filterTiers(JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> indexRDD, Byte[] tierIds) {
		return indexRDD.filter(v1 -> Arrays.asList(tierIds).contains(v1._1().getBytes()[0]));
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
				
				List<ByteArrayId> insertIds = broadcastStrategy.value().reprojectToCoarserTier(t._1(), targetTierId);
				
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
	
	private JavaPairRDD<GeoWaveInputKey, ByteArrayId> joinAndCompareTiers(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> leftTier,
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, byte[]>> rightTier,
			GeomFunction predicate) {
		//Cogroup looks at each RDD and grab keys that are the same in this case ByteArrayId
		JavaPairRDD<GeoWaveInputKey, ByteArrayId> finalMatches = null;
		JavaPairRDD<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, byte[]>>, Iterable<Tuple2<GeoWaveInputKey, byte[]>>>> joinedTiers = leftTier.cogroup(rightTier);
		
		//We need to go through the pairs and test each feature against each other
		//End with a combined RDD for that tier.
		finalMatches = joinedTiers.flatMapValues(new Function<Tuple2<Iterable<Tuple2<GeoWaveInputKey, byte[]>>, Iterable<Tuple2<GeoWaveInputKey, byte[]>>>, Iterable<GeoWaveInputKey>>() {

			@Override
			public Iterable<GeoWaveInputKey> call(
					Tuple2<Iterable<Tuple2<GeoWaveInputKey, byte[]>>, Iterable<Tuple2<GeoWaveInputKey, byte[]>>> t )
					throws Exception {
				List<GeoWaveInputKey> resultPairs = new ArrayList<>();
				
				Iterable<Tuple2<GeoWaveInputKey, byte[]>> leftFeatures = t._1;
				Iterable<Tuple2<GeoWaveInputKey, byte[]>> rightFeatures = t._2;
				
				if(Iterators.size(rightFeatures.iterator()) == 0) {
					return resultPairs;
				}
				//Compare each filtered set against one another and add feature pairs that
				for (Tuple2<GeoWaveInputKey, byte[]> leftTuple : leftFeatures) {
					
					for (Tuple2<GeoWaveInputKey, byte[]> rightTuple : rightFeatures ) {
						
						if(predicate.call(leftTuple._2, rightTuple._2)) {
							resultPairs.add(leftTuple._1);
							resultPairs.add(rightTuple._1);
						}
						
					}
					
				}
				
				return resultPairs;
			}}).mapToPair(t -> t.swap());
		
		finalMatches = finalMatches.reduceByKey((idLeft, idRight) -> idLeft);
		
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
		return (left) ? leftJoined.keys().count() : rightJoined.keys().count();
	}

}