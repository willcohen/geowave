package mil.nga.giat.geowave.analytic.spark.spatial;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomDistance;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomWithinDistance;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;
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
	private JavaPairRDD<GeoWaveInputKey, Geometry> combinedResults = null;
	
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
			NumericIndexStrategy indexStrategy) throws InterruptedException, ExecutionException {
		//Get SparkContext from session
		SparkContext sc = spark.sparkContext();
		
		TieredSFCIndexStrategy tieredStrategy = (TieredSFCIndexStrategy) indexStrategy;
		SubStrategy[] tierStrategies = tieredStrategy.getSubStrategies();
		int tierCount = tierStrategies.length;
		ClassTag<TieredSFCIndexStrategy> tieredClassTag = scala.reflect.ClassTag$.MODULE$.apply(TieredSFCIndexStrategy.class);

		ClassTag<GeomFunction> geomFuncClassTag = scala.reflect.ClassTag$.MODULE$.apply(predicate.getClass());
		//Create broadcast variable for indexing strategy
		Broadcast<TieredSFCIndexStrategy> broadcastStrategy = sc.broadcast(tieredStrategy, tieredClassTag );
		
		Broadcast<GeomFunction> geomPredicate = sc.broadcast(predicate, geomFuncClassTag);
		
		//Generate Index RDDs for each set of data.
		double bufferDistance = 0.0;
		if(predicate instanceof GeomWithinDistance) {
			bufferDistance = ((GeomWithinDistance)predicate).getRadius();
		}
		
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftIndex = this.indexData(leftRDD, broadcastStrategy, bufferDistance);
		JavaFutureAction<List<Byte>> leftFuture = leftIndex.keys().map(id -> id.getBytes()[0]).distinct(1).collectAsync();
		
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rightIndex = this.indexData(rightRDD, broadcastStrategy, bufferDistance);
		JavaFutureAction<List<Byte>> rightFuture = rightIndex.keys().map(id -> id.getBytes()[0]).distinct(1).collectAsync();
		
		rightDataTiers = rightFuture.get();
		leftDataTiers = leftFuture.get();
		Map<Byte, HashSet<Byte>> rightReprojectMap = new HashMap<Byte, HashSet<Byte>>();
		Map<Byte, ArrayList<Byte>> leftReprojectMap = new HashMap<Byte, ArrayList<Byte>>();
		for(Byte tierLeft : leftDataTiers) {
			ArrayList<Byte> higherTiers = new ArrayList<Byte>();
			for(Byte tierRight : rightDataTiers) {
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
				leftReprojectMap.put(tierLeft, higherTiers);
			}
		}
		int leftTierCount = leftDataTiers.size();
		int rightTierCount = rightDataTiers.size();
		LOGGER.warn("Tier Count: " + tierCount);
		LOGGER.warn("Left Tier Count: " + leftTierCount + " Right Tier Count: " + rightTierCount);
		LOGGER.warn("Left Tiers: " + leftDataTiers);
		LOGGER.warn("Right Tiers: " + rightDataTiers);
		
		for(Byte leftTierId : leftDataTiers) {
			
			ArrayList<Byte> rightHigherTiers = leftReprojectMap.get(leftTierId);

			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftTier = null;
			if (rightHigherTiers != null) {

				leftTier = this.filterTier(leftIndex, leftTierId);
				//Filter and reproject right tiers to coarser left tier
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> higherRightTiers = filterTiers(rightIndex, rightHigherTiers);
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> reprojected = reprojectToTier(higherRightTiers, leftTierId, broadcastStrategy);
				//reprojected.checkpoint();
				JavaPairRDD<GeoWaveInputKey, Geometry> finalTierMatches = this.joinAndCompareTiers(leftTier, reprojected, geomPredicate);
				
				this.addMatches(finalTierMatches);
			}
			
			if(rightDataTiers.contains(leftTierId)) {
				//Filter and join and same level
				leftTier = this.filterTier(leftIndex, leftTierId);
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey,Geometry>> rightTier = this.filterTier(rightIndex, leftTierId);
				JavaPairRDD<GeoWaveInputKey, Geometry> finalTierMatches = this.joinAndCompareTiers(leftTier, rightTier, geomPredicate);
				
				this.addMatches(finalTierMatches);
			}
			
			if(leftTier != null) {
				leftTier.unpersist();
			}
		}
		
	for(Byte rightTierId : rightDataTiers) {
			HashSet<Byte> higherLeftTiers = rightReprojectMap.get(rightTierId);
			if(higherLeftTiers != null) {
				ArrayList<Byte> tiers = new ArrayList<Byte>(higherLeftTiers);
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rightTier = this.filterTier(rightIndex, rightTierId);
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> filteredLeftTiers = filterTiers(leftIndex, tiers );
				JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> reprojected = reprojectToTier(filteredLeftTiers, rightTierId, broadcastStrategy);
				//reprojected.checkpoint();
				JavaPairRDD<GeoWaveInputKey, Geometry> finalTierMatches = this.joinAndCompareTiers(reprojected, rightTier, geomPredicate);
				
				this.addMatches(finalTierMatches);
			}
		}
	
		//Remove duplicates between tiers
		this.combinedResults = this.combinedResults.reduceByKey((f1,f2) -> f1);
		//Cache results
		this.combinedResults = this.combinedResults.cache();

		//Join against original dataset to give final joined rdds on each side
		this.setLeftJoined(
				leftRDD.join(this.combinedResults).mapToPair( t -> new Tuple2<GeoWaveInputKey,SimpleFeature>(t._1(),t._2._1()) ));
		this.setRightJoined(
				rightRDD.join(this.combinedResults).mapToPair( t -> new Tuple2<GeoWaveInputKey,SimpleFeature>(t._1(),t._2._1()) ));
		
		//Finally mark the final joined set on each side as cached so it doesn't recalculate work.
		leftJoined.cache();
		rightJoined.cache();
	}

	private void addMatches(JavaPairRDD<GeoWaveInputKey, Geometry> finalTierMatches) {
		if(this.combinedResults == null) {
			this.combinedResults = finalTierMatches;
		} else {
			this.combinedResults = this.combinedResults.union(finalTierMatches);
		}
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexData(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> data,
			Broadcast<TieredSFCIndexStrategy> broadcastStrategy, double bufferDistance)
	{
		//Flat map is used because each pair can potentially yield 1+ output rows within rdd.
		//Instead of storing whole feature on index maybe just output Key + Bounds
		JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexedData = data.flatMapToPair(new PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>,ByteArrayId, Tuple2<GeoWaveInputKey,Geometry>>() {
			@Override
			public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> call(
					Tuple2<GeoWaveInputKey, SimpleFeature> t )
					throws Exception {
				
				//Flattened output array.
				List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> result = new ArrayList<>();


				//Pull feature to index from tuple
				SimpleFeature inputFeature = t._2;
				
				Geometry geom = (Geometry)inputFeature.getDefaultGeometry();
				if(geom == null) {
					return result.iterator();
				}
				
				//Extract bounding box from input feature
				BoundingBox bounds = inputFeature.getBounds();
				NumericRange xRange = new NumericRange(bounds.getMinX() - bufferDistance, bounds.getMaxX() + bufferDistance);
				NumericRange yRange = new NumericRange(bounds.getMinY() - bufferDistance, bounds.getMaxY() + bufferDistance);
				
				if(bounds.isEmpty()) {
					Envelope internalEnvelope = geom.getEnvelopeInternal();
					xRange = new NumericRange(internalEnvelope.getMinX() - bufferDistance, internalEnvelope.getMaxX() + bufferDistance);
					yRange = new NumericRange(internalEnvelope.getMinY() - bufferDistance, internalEnvelope.getMaxY() + bufferDistance);
				
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
					Tuple2<GeoWaveInputKey, Geometry> valuePair = new Tuple2<>(t._1, geom);
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey,Geometry>>(id, valuePair );
					result.add(indexPair);
				}
				
				return result.iterator();
			}
			
		});		
		return indexedData;
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> filterTier(JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftFilteredTiers, byte tierId) {
		return leftFilteredTiers.filter(v1 -> v1._1().getBytes()[0] == tierId);	
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> filterTiers(JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftIndex, List<Byte> tierIds) {
		return leftIndex.filter(v1 -> tierIds.contains(v1._1().getBytes()[0]));
	}
	
	private JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> reprojectToTier(JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftTier, 
			byte targetTierId,
			Broadcast<TieredSFCIndexStrategy> broadcastStrategy) {
		return leftTier.flatMapToPair(new PairFlatMapFunction<Tuple2<ByteArrayId,Tuple2<GeoWaveInputKey,Geometry>>,ByteArrayId,Tuple2<GeoWaveInputKey, Geometry>>() {

			@Override
			public Iterator<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> call(
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> t )
					throws Exception {
				
				List<Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>> reprojected = new ArrayList<>();
				
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
				
				//List<ByteArrayId> insertIds = broadcastStrategy.value().reprojectToCoarserTier(t._1(), targetTierId);
				Geometry geom = t._2._2;
				
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
					Tuple2<GeoWaveInputKey, Geometry> valuePair = new Tuple2<>(t._2._1, t._2._2);
					Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> indexPair = new Tuple2<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>>(id, valuePair);
					reprojected.add(indexPair);
				}
				
				return reprojected.iterator();
			}
			
		});
	}
	
	private JavaPairRDD<GeoWaveInputKey, Geometry> joinAndCompareTiers(
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> leftTier,
			JavaPairRDD<ByteArrayId, Tuple2<GeoWaveInputKey, Geometry>> rightTier,
			Broadcast<GeomFunction> geomPredicate) {
		//Cogroup looks at each RDD and grab keys that are the same in this case ByteArrayId
		JavaPairRDD<GeoWaveInputKey, Geometry> finalMatches = null;
		
		JavaPairRDD<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, Geometry>>, Iterable<Tuple2<GeoWaveInputKey, Geometry>>>> joinedTiers = leftTier.cogroup(rightTier);
		
		joinedTiers = joinedTiers.filter(t -> (t._2._1.iterator().hasNext() && t._2._2.iterator().hasNext()));
		
		//We need to go through the pairs and test each feature against each other
		//End with a combined RDD for that tier.;
		finalMatches = joinedTiers.flatMapToPair(new PairFlatMapFunction<Tuple2<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey,Geometry>>,Iterable<Tuple2<GeoWaveInputKey,Geometry>>>>,GeoWaveInputKey, Geometry>() {

			@Override
			public Iterator<Tuple2<GeoWaveInputKey, Geometry>> call(
					Tuple2<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, Geometry>>, Iterable<Tuple2<GeoWaveInputKey, Geometry>>>> t )
					throws Exception {

				ArrayList<Tuple2<GeoWaveInputKey, Geometry>> resultSet = Lists.newArrayList();
				for(Tuple2<GeoWaveInputKey, Geometry> leftTuple : t._2._1) {
					for(Tuple2<GeoWaveInputKey, Geometry> rightTuple : t._2._2) {
						if(geomPredicate.value().call(leftTuple._2, rightTuple._2)) {
							resultSet.add(leftTuple);
							resultSet.add(rightTuple);
						}
					}
				}
				return resultSet.iterator();
			}});
		
		finalMatches = finalMatches.reduceByKey((idLeft, idRight) -> idLeft);
		
		finalMatches.cache();
		
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