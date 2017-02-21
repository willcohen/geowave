package mil.nga.giat.geowave.core.store.index.text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.index.FieldIndexStrategy;

public class TextIndexStrategy implements
		FieldIndexStrategy<TextQueryConstraint, String>
{
	private static final String ID = "TEXT";

	public TextIndexStrategy() {
		super();
	}

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	@Override
	public QueryRanges getQueryRanges(
			TextQueryConstraint indexedRange,
			IndexMetaData... hints ) {
		return indexedRange.getQueryRanges();
	}

	@Override
	public QueryRanges getQueryRanges(
			TextQueryConstraint indexedRange,
			int maxEstimatedRangeDecomposition,
			IndexMetaData... hints ) {
		return getQueryRanges(
				indexedRange,
				hints);
	}

	@Override
	public InsertionIds getInsertionIds(
			List<FieldInfo<String>> indexedData ) {
		final List<ByteArrayId> sortKeys = new ArrayList<>();
		for (FieldInfo<String> fieldInfo : indexedData) {
			sortKeys.add(new ByteArrayId(
					fieldInfo.getDataValue().getValue()));
		}
		return new InsertionIds(null, sortKeys);
	}

	@Override
	public InsertionIds getInsertionIds(
			List<FieldInfo<String>> indexedData,
			int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public List<FieldInfo<String>> getRangeForId(
			ByteArrayId partitionKey,
			ByteArrayId sortKey ) {
		return Collections.emptyList();
	}
}
