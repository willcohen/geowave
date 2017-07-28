package mil.nga.giat.geowave.example.cli;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class ExampleOperationProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		ExampleSection.class,
		ExampleHBaseServerCommand.class,
		ExampleAccumuloServerCommand.class,
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}