package mil.nga.giat.geowave.core.store.index.temporal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.index.FieldIndexStrategy;

public class TemporalIndexStrategy implements
		FieldIndexStrategy<TemporalQueryConstraint, Date>
{
	private static final String ID = "TEMPORAL";

	public TemporalIndexStrategy() {
		super();
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	public static final byte[] toIndexByte(
			final Date date ) {
		return Lexicoders.LONG.toByteArray(date.getTime());
	}


	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	@Override
	public QueryRanges getQueryRanges(
			TemporalQueryConstraint indexedRange,
			IndexMetaData... hints ) {
		return indexedRange.getQueryRanges();
	}

	@Override
	public QueryRanges getQueryRanges(
			TemporalQueryConstraint indexedRange,
			int maxEstimatedRangeDecomposition,
			IndexMetaData... hints ) {
		return getQueryRanges(indexedRange);
	}

	@Override
	public InsertionIds getInsertionIds(
			List<FieldInfo<Date>> indexedData ) {
		final List<ByteArrayId> sortKeys = new ArrayList<>();
		for (final FieldInfo<Date> fieldInfo : indexedData) {
			sortKeys.add(new ByteArrayId(
					toIndexByte(fieldInfo.getDataValue().getValue())));
		}
		return new InsertionIds(sortKeys);
	}

	@Override
	public InsertionIds getInsertionIds(
			List<FieldInfo<Date>> indexedData,
			int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public List<FieldInfo<Date>> getRangeForId(
			ByteArrayId partitionKey,
			ByteArrayId sortKey ) {
		return Collections.emptyList();
	}

}
