package mil.nga.giat.geowave.core.store.entities;

public interface GeoWaveKey
{
	public byte[] getDataId();

	public byte[] getAdapterId();

	public byte[] getSortKey();
	
	public byte[] getPartitionKey();

	public int getNumberOfDuplicates();
}
