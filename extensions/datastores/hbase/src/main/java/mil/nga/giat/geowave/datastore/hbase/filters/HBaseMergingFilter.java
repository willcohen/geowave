package mil.nga.giat.geowave.datastore.hbase.filters;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

public class HBaseMergingFilter extends
		FilterBase
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			HBaseMergingFilter.class);

	private String mergeData;

	public HBaseMergingFilter() {

	}

	public static HBaseMergingFilter parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		HBaseMergingFilter mergingFilter = new HBaseMergingFilter();

		String mergeData = StringUtils.stringFromBinary(pbBytes);
		mergingFilter.setMergeData(mergeData);

		return mergingFilter;
	}
	
	@Override
	public byte[] toByteArray()
			throws IOException {
		return StringUtils.stringToBinary(mergeData);
	}
	
	/**
	 * Enable filterRowCells
	 */
	@Override
	public boolean hasFilterRow() {
		return true;
	}

	/**
	 * Handle the entire row at one time
	 */
	@Override
	public void filterRowCells(
			List<Cell> rowCells )
			throws IOException {
		if (!rowCells.isEmpty()) {			
			if (rowCells.size() > 1) {
				Cell firstCell = rowCells.get(0);
				byte[] singleRow = CellUtil.cloneRow(firstCell);
				byte[] singleFam = CellUtil.cloneFamily(firstCell);
				byte[] singleQual = CellUtil.cloneQualifier(firstCell);
				
				Mergeable mergedValue = null;
				for (Cell cell : rowCells) {
					byte[] byteValue = CellUtil.cloneValue(
							cell);
					Mergeable value = (Mergeable) PersistenceUtils.fromBinary(
							byteValue,
							Mergeable.class);

					if (mergedValue != null) {
						mergedValue.merge(
								value);
					}
					else {
						mergedValue = value;
					}
				}

				Cell singleCell = CellUtil.createCell(
						singleRow,
						singleFam,
						singleQual,
						System.currentTimeMillis(),
						KeyValue.Type.Put.getCode(),
						PersistenceUtils.toBinary(mergedValue));
				
				rowCells.clear();
				rowCells.add(singleCell);
			}
		}
	}

	/**
	 * Don't do anything special here, since we're only interested in whole rows
	 */
	@Override
	public ReturnCode filterKeyValue(
			Cell v )
			throws IOException {
		return ReturnCode.INCLUDE;
	}

	public String getMergeData() {
		return mergeData;
	}

	public void setMergeData(
			String mergeData ) {
		this.mergeData = mergeData;
	}
}
