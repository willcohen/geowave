package mil.nga.giat.geowave.core.store.base;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

/**
 * There is a single intermediate row per original entry passed into a write
 * operation. This offers a higher level abstraction from the raw key-value
 * pairs in geowave (can be multiple per original entry). A datastore is
 * responsible for translating from this intermediary representation of rows to
 * key-value rows.
 *
 */
public class IntermediaryWriteEntryInfo
{
	public static class FieldInfo<T>
	{
		private final PersistentValue<T> dataValue;
		private final byte[] visibility;
		private final byte[] writtenValue;

		public FieldInfo(
				final PersistentValue<T> dataValue,
				final byte[] writtenValue,
				final byte[] visibility ) {
			this.dataValue = dataValue;
			this.writtenValue = writtenValue;
			this.visibility = visibility;
		}

		public PersistentValue<T> getDataValue() {
			return dataValue;
		}

		public byte[] getWrittenValue() {
			return writtenValue;
		}

		public byte[] getVisibility() {
			return visibility;
		}
	}

	private final byte[] dataId;
	private final byte[] adapterId;
	private final InsertionIds insertionIds;
	private final List<FieldInfo<?>> fieldInfo;

	public IntermediaryWriteEntryInfo(
			final byte[] dataId,
			final byte[] adapterId,
			final InsertionIds insertionIds,
			final List<FieldInfo<?>> fieldInfo ) {
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.insertionIds = insertionIds;
		this.fieldInfo = fieldInfo;
	}

	@Override
	public String toString() {
		return new ByteArrayId(
				dataId).getString();
	}

	public byte[] getAdapterId() {
		return adapterId;
	}

	public InsertionIds getInsertionIds() {
		return insertionIds;
	}

	public byte[] getDataId() {
		return dataId;
	}

	public List<FieldInfo<?>> getFieldInfo() {
		return fieldInfo;
	}

}
