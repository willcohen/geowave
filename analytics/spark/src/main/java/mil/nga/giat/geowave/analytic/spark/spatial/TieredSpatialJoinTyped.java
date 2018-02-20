package mil.nga.giat.geowave.analytic.spark.spatial;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.mail.Session;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.analytic.spark.sparksql.util.GeomReader;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.SingleTierSubStrategy;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class TieredSpatialJoinTyped implements SpatialJoin {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(TieredSpatialJoinTyped.class);
	
	//Combined matching pairs
	public Dataset<CommonIndexType> joinResults = null;
	
	List<Tuple2<Byte, Dataset<CommonIndexType>>> leftDataTiers = new ArrayList<>();
	List<Tuple2<Byte, Dataset<CommonIndexType>>> rightDataTiers = new ArrayList<>();
	
	//Final joined pair RDDs
	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftJoined = null;
	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightJoined = null;
	
	public TieredSpatialJoinTyped() { }
	
	@Override
	public void join(
			SparkSession spark,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftRDD,
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightRDD,
			GeomFunction predicate,
			NumericIndexStrategy indexStrategy) {
		
		SparkContext sc = spark.sparkContext();
		SpatialDimensionalityTypeProvider provider = new SpatialDimensionalityTypeProvider();
		PrimaryIndex index = provider.createPrimaryIndex();
		TieredSFCIndexStrategy strategy = (TieredSFCIndexStrategy) index.getIndexStrategy();
		
		ClassTag<TieredSFCIndexStrategy> tieredClassTag = scala.reflect.ClassTag$.MODULE$.apply(TieredSFCIndexStrategy.class);
		Broadcast<TieredSFCIndexStrategy> broadcastStrategy = sc.broadcast(strategy, tieredClassTag);
		
		//Generate Index RDDs for each set of data.
		IndexedDataset leftData = new IndexedDataset(spark, new TieredIndexMapper());
		IndexedDataset rightData = new IndexedDataset(spark, new TieredIndexMapper());

		Dataset<CommonIndexType> leftFrame = leftData.getDataFrame(leftRDD);
		Dataset<CommonIndexType> rightFrame = rightData.getDataFrame(rightRDD);
		
		//leftFrame.cache();
		//rightFrame.cache();
		
		//This function creates a list of tiers that actually contain data for each set.
		this.collectDataTiersTyped(leftFrame, rightFrame, strategy);
		
		//Iterate through tiers and join each tier containing data from each set.
		for(Tuple2<Byte, Dataset<CommonIndexType>> t : this.leftDataTiers) {
			Byte leftTierId = t._1();
			
			//Filter left feature set for tier
			Dataset<CommonIndexType> leftTier = t._2();
			leftTier.cache();
			
			for(Tuple2<Byte, Dataset<CommonIndexType>> t2 : this.rightDataTiers) {
				//Filter the tier from right dataset
				Byte rightTierId = t2._1();
				Dataset<CommonIndexType> rightTier = t2._2();
				
				//We found a tier on the right with geometry to test against.
				//Reproject one of the data sets to the coarser tier
				if(leftTierId > rightTierId) {
					leftTier = this.reprojectToTierTyped(leftTier, rightTierId, broadcastStrategy);
					leftTier.cache();
				} else if (leftTierId < rightTierId) {
					rightTier = this.reprojectToTierTyped(rightTier, leftTierId, broadcastStrategy);
					rightTier.cache();
				}

				//Once we have each tier and index at same resolution then join and compare each of the sets
				Dataset<CommonIndexType> finalTierMatches = this.joinAndCompareTiersTyped(leftTier,rightTier, predicate);
				
				//Combine each tier into a final list of matches for all tiers
				if(this.joinResults == null) {
					this.joinResults = finalTierMatches;
				} else {
					this.joinResults = this.joinResults.union(finalTierMatches);
				}
			}

			leftTier.unpersist();
		}
		
		this.joinResults.cache();
	
		//Remove duplicates
		this.joinResults = this.joinResults.dropDuplicates(new String[] {"adapterId","dataId"});
		//this.leftJoined = this.joinResults.join(leftRDD).mapToPair( t -> new Tuple2<GeoWaveInputKey,SimpleFeature>(t._1(),t._2._2()) );
		//this.rightJoined = this.joinResults.join(rightRDD).mapToPair( t -> new Tuple2<GeoWaveInputKey,SimpleFeature>(t._1(),t._2._2()) );

		
	}
	
	private void collectDataTiersTyped(
			Dataset<CommonIndexType> leftFrame,
			Dataset<CommonIndexType> rightFrame,
			TieredSFCIndexStrategy strategy) {
		
		SubStrategy[] tierStrategies = strategy.getSubStrategies();
		int tierCount = tierStrategies.length;
		byte minTierId = (byte) 0;
		byte maxTierId = (byte)(tierCount - 1);
		
		leftFrame.select(leftFrame.col("insertionId"));
		
		for(int iTier = maxTierId; iTier >= minTierId; iTier--) {
			SingleTierSubStrategy tierStrategy = (SingleTierSubStrategy) tierStrategies[iTier].getIndexStrategy();
			byte tierId = tierStrategy.tier;
			Dataset<CommonIndexType> leftTier = this.filterTierTyped(leftFrame, tierId);
			Dataset<CommonIndexType> rightTier = this.filterTierTyped(rightFrame, tierId);
			
			if(leftTier.takeAsList(1).size() > 0) {
				Tuple2<Byte, Dataset<CommonIndexType>> pair = new Tuple2<>(tierId, leftTier);

				this.leftDataTiers.add(pair);
			}
			
			if(rightTier.takeAsList(1).size() > 0) {
				Tuple2<Byte, Dataset<CommonIndexType>> pair = new Tuple2<>(tierId, rightTier);

				this.rightDataTiers.add(pair);
			}
			
		}
	}
	
	private Dataset<CommonIndexType> filterTierTyped(Dataset<CommonIndexType> leftFrame, byte tierId) {
		return leftFrame.filter(indexed -> (indexed.getInsertionId()[0] == tierId));
	}
	
	private Dataset<CommonIndexType> reprojectToTierTyped(Dataset<CommonIndexType> leftTier, 
			byte targetTierId,
			Broadcast<TieredSFCIndexStrategy> broadcastStrategy) {

		Dataset<CommonIndexType> reprojected = leftTier.flatMap(new FlatMapFunction<CommonIndexType,CommonIndexType>() {

			@Override
			public Iterator<CommonIndexType> call(
					CommonIndexType t )
					throws Exception {
				
				List<CommonIndexType> reprojected = new ArrayList<>();
				
				SpatialDimensionalityTypeProvider provider = new SpatialDimensionalityTypeProvider();
				PrimaryIndex index = provider.createPrimaryIndex();
				TieredSFCIndexStrategy strategy = (TieredSFCIndexStrategy) index.getIndexStrategy();

				SubStrategy[] strats = strategy.getSubStrategies();
				
				if(strategy.tierExists(targetTierId) == false) {
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
				byte[] geomString = t.getGeom();
				Geometry geom = reader.read(geomString);
				
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
					
					reprojected.add(new CommonIndexType(id.getBytes(), t.getAdapterId(), t.getDataId(), geomString));
				}
				
				return reprojected.iterator();
			}
			
			}, Encoders.bean(CommonIndexType.class));
		 
		 return reprojected;
	}
	
	private Dataset<CommonIndexType> joinAndCompareTiersTyped(
			Dataset<CommonIndexType> leftTier,
			Dataset<CommonIndexType> rightTier,
			GeomFunction predicate) {
		//Cogroup looks at each RDD and grab keys that are the same in this case ByteArrayId
		
		//JavaPairRDD<GeoWaveInputKey, String> finalMatches = null;
		Dataset<CommonIndexType> finalMatches = null;
		//JavaPairRDD<ByteArrayId, Tuple2<Iterable<Tuple2<GeoWaveInputKey, String>>, Iterable<Tuple2<GeoWaveInputKey, String>>>> joinedTiers = leftTier.cogroup(rightTier);
		Dataset<Tuple2<CommonIndexType, CommonIndexType>> joinedTiers = leftTier.joinWith(rightTier, leftTier.col("insertionId").equalTo(rightTier.col("insertionId")));
		//We need to go through the pairs and test each feature against each other
		//End with a combined RDD for that tier.
		finalMatches = joinedTiers.flatMap(new FlatMapFunction<Tuple2<CommonIndexType,CommonIndexType>, CommonIndexType>() {

			@Override
			public Iterator<CommonIndexType> call(
					Tuple2<CommonIndexType, CommonIndexType> t )
					throws Exception {

				List<CommonIndexType> resultPairs = new ArrayList<>();
				
				CommonIndexType leftFeature = t._1();
				CommonIndexType rightFeature = t._2();
				
				if(leftFeature == null || rightFeature == null) {
					return resultPairs.iterator();
				}
				
				byte[] leftGeom = leftFeature.getGeom();
				byte[] rightGeom = rightFeature.getGeom();
				if(predicate.call(leftGeom, rightGeom)) {
					resultPairs.add(leftFeature);
					resultPairs.add(rightFeature);
				}
				
				return resultPairs.iterator();
			} }, Encoders.bean(CommonIndexType.class));

		//Remove duplicates from previous step
		//finalMatches = finalMatches.reduceByKey((s1,s2) -> s1);		
		
		return finalMatches;
	}
	
}