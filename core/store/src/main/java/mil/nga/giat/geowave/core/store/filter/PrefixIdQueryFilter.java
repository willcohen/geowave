package mil.nga.giat.geowave.core.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class PrefixIdQueryFilter implements
		DistributableQueryFilter
{
	private ByteArrayId sortKeyPrefix;

	protected PrefixIdQueryFilter() {}

	public PrefixIdQueryFilter(
			final ByteArrayId sortKeyPrefix ) {
		this.sortKeyPrefix = sortKeyPrefix;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		ByteArrayId sortKey = persistenceEncoding.getInsertionSortKey();
		return (Arrays.equals(
				sortKeyPrefix.getBytes(),
				Arrays.copyOf(
						sortKey.getBytes(),
						sortKeyPrefix.getBytes().length)));
	}

	@Override
	public byte[] toBinary() {
		return sortKeyPrefix.getBytes();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		sortKeyPrefix = new ByteArrayId(
				bytes);
	}

}
