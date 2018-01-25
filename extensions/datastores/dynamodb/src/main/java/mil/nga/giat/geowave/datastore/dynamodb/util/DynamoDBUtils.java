package mil.nga.giat.geowave.datastore.dynamodb.util;

import java.io.Closeable;

public class DynamoDBUtils
{
	public static class NoopClosableIteratorWrapper implements
			Closeable
	{
		public NoopClosableIteratorWrapper() {}

		@Override
		public void close() {
		}
	}
}
