package mil.nga.giat.geowave.core.store.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.base.IntermediaryWriteEntryInfo;
import mil.nga.giat.geowave.core.store.base.IntermediaryWriteEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

/**
 * One manager associated with each primary index.
 *
 *
 * @param <T>
 *            The type of entity being indexed
 */
public class SecondaryIndexDataManager<T> implements
		Closeable,
		IngestCallback<T, GeoWaveRow>,
		DeleteCallback<T, GeoWaveRow>
{
	private final SecondaryIndexDataAdapter<T> adapter;
	final SecondaryIndexDataStore secondaryIndexStore;
	final ByteArrayId primaryIndexId;

	public SecondaryIndexDataManager(
			final SecondaryIndexDataStore secondaryIndexStore,
			final SecondaryIndexDataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		this.adapter = adapter;
		this.secondaryIndexStore = secondaryIndexStore;
		this.primaryIndexId = primaryIndexId;

	}

	@Override
	public void entryIngested(
			final T entry,
			GeoWaveRow... kvs) {
		// loop secondary indices for adapter
		for (final SecondaryIndex<T> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			final ByteArrayId indexedAttributeFieldId = secondaryIndex.getFieldId();
			// get fieldInfo for fieldId to be indexed
			final FieldInfo<?> indexedAttributeFieldInfo = getFieldInfo(
					entryInfo,
					indexedAttributeFieldId);
			// get indexed value(s) for current field
			@SuppressWarnings("unchecked")
			final InsertionIds secondaryIndexInsertionIds = secondaryIndex.getIndexStrategy().getInsertionIds(
					Arrays.asList(
							indexedAttributeFieldInfo));
			// loop insertionIds
			for (final ByteArrayId insertionId : secondaryIndexInsertionIds.getCompositeInsertionIds()) {
				final ByteArrayId attributeVisibility = new ByteArrayId(
						indexedAttributeFieldInfo.getVisibility());
				final ByteArrayId dataId = new ByteArrayId(
						entryInfo.getDataId());
				switch (secondaryIndex.getSecondaryIndexType()) {
					case JOIN:
						final InsertionIds primaryIndexInsertionIds = entryInfo.getInsertionIds();
						final Pair<ByteArrayId, ByteArrayId> firstPartitionAndSortKey = primaryIndexInsertionIds
								.getFirstPartitionAndSortKeyPair();
						secondaryIndexStore.storeJoinEntry(
								secondaryIndex.getId(),
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								firstPartitionAndSortKey.getLeft(),
								firstPartitionAndSortKey.getRight(),
								attributeVisibility);
						break;
					case PARTIAL:
						final List<FieldInfo<?>> attributes = new ArrayList<>();
						final List<ByteArrayId> attributesToStore = secondaryIndex.getPartialFieldIds();
						for (final ByteArrayId fieldId : attributesToStore) {
							attributes.add(
									getFieldInfo(
											entryInfo,
											fieldId));
						}
						secondaryIndexStore.storeEntry(
								secondaryIndex.getId(),
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								attributeVisibility,
								attributes);
						break;
					case FULL:
						secondaryIndexStore.storeEntry(
								secondaryIndex.getId(),
								insertionId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								attributeVisibility,
								// full simply sends over all of the
								// attributes
								entryInfo.getFieldInfo());
						break;
					default:
						break;
				}
			}
			// capture statistics
			for (final DataStatistics<T> associatedStatistic : secondaryIndex.getAssociatedStatistics()) {
				associatedStatistic.entryIngested(
						entryInfo,
						entry);
			}
		}
	}

	@Override
	public void entryDeleted(
			final T entry,
			GeoWaveRow... kv ) {
		// loop secondary indices for adapter
		for (final SecondaryIndex<T> secondaryIndex : adapter.getSupportedSecondaryIndices()) {
			final ByteArrayId indexedAttributeFieldId = secondaryIndex.getFieldId();
			// get fieldInfo for fieldId to be deleted
			final FieldInfo<?> indexedAttributeFieldInfo = getFieldInfo(
					entryInfo,
					indexedAttributeFieldId);
			// get indexed value(s) for current field
			@SuppressWarnings("unchecked")
			final InsertionIds secondaryIndexRowIds = secondaryIndex.getIndexStrategy().getInsertionIds(
					Arrays.asList(
							indexedAttributeFieldInfo));
			// loop insertionIds
			for (final ByteArrayId secondaryIndexRowId : secondaryIndexRowIds.getCompositeInsertionIds()) {
				final ByteArrayId dataId = new ByteArrayId(
						entryInfo.getDataId());
				switch (secondaryIndex.getSecondaryIndexType()) {
					case JOIN:
						final InsertionIds primaryIndexInsertionIds = entryInfo.getInsertionIds();
						final Pair<ByteArrayId, ByteArrayId> firstPartitionAndSortKey = primaryIndexInsertionIds
								.getFirstPartitionAndSortKeyPair();
						secondaryIndexStore.deleteJoinEntry(
								secondaryIndex.getId(),
								secondaryIndexRowId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								firstPartitionAndSortKey.getLeft(),
								firstPartitionAndSortKey.getRight());
						break;
					case PARTIAL:
						final List<FieldInfo<?>> attributes = new ArrayList<>();
						final List<ByteArrayId> attributesToDelete = secondaryIndex.getPartialFieldIds();
						for (final ByteArrayId fieldId : attributesToDelete) {
							attributes.add(
									getFieldInfo(
											entryInfo,
											fieldId));
						}
						secondaryIndexStore.deleteEntry(
								secondaryIndex.getId(),
								secondaryIndexRowId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								attributes);
						break;
					case FULL:
						secondaryIndexStore.deleteEntry(
								secondaryIndex.getId(),
								secondaryIndexRowId,
								adapter.getAdapterId(),
								indexedAttributeFieldId,
								dataId,
								// full simply sends over all of the
								// attributes
								entryInfo.getFieldInfo());
						break;
					default:
						break;
				}
			}
			// TODO delete statistics
			// for (final DataStatistics<T> associatedStatistic :
			// secondaryIndex.getAssociatedStatistics()) {
			// associatedStatistic.entryDeleted(
			// entryInfo,
			// entry);
			// }
		}

	}

	private FieldInfo<?> getFieldInfo(
			final IntermediaryWriteEntryInfo entryInfo,
			final ByteArrayId fieldID ) {
		for (final FieldInfo<?> info : entryInfo.getFieldInfo()) {
			if (info.getDataValue().getId().equals(
					fieldID)) {
				return info;
			}
		}
		return null;
	}

	@Override
	public void close()
			throws IOException {
		if (secondaryIndexStore != null) {
			secondaryIndexStore.flush();
		}
	}

}