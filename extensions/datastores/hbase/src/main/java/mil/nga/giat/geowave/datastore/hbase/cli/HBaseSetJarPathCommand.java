package mil.nga.giat.geowave.datastore.hbase.cli;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderPrefixTranslator;
import mil.nga.giat.geowave.core.cli.prefix.JCommanderTranslationMap;
import mil.nga.giat.geowave.core.cli.prefix.TranslationEntry;

@GeowaveOperation(name = "jarpath", parentOperation = HBaseSection.class)
@Parameters(commandDescription = "Set the jar path for coprocessor registration")

public class HBaseSetJarPathCommand extends
		ServiceEnabledCommand<String>
{
	@Parameter(description = "<Jar Path>")
	private List<String> parameters = new ArrayList<String>();
	private String jarPath = null;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		JCommander.getConsole().println(
				computeResults(
						params));
	}

	@Override
	public String computeResults(
			OperationParams params )
			throws Exception {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <GeoServer URL>");
		}
		jarPath = parameters.get(
				0);
		final Properties existingProps = getGeoWaveConfigProperties(
				params);

		// all switches are optional
		if (jarPath != null) {
			existingProps.setProperty(
					HBaseConstants.HBASE_JAR_PATH,
					jarPath);
		}

		// Write properties file
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(
						params),
				existingProps,
				this.getClass(),
				HBaseConstants.HBASE_NAMESPACE_PREFIX);

		// generate a return for rest calls
		StringBuilder builder = new StringBuilder();
		for (Object key : existingProps.keySet()) {
			if (key.toString().startsWith(
					"hbase")) {
				builder.append(
						key.toString() + "=" + existingProps.getProperty(
								key.toString()) + "\n");
			}
		}

		return builder.toString();
	}

	public String usage() {
		StringBuilder builder = new StringBuilder();

		final List<String> nameArray = new ArrayList<String>();
		final JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		translator.addObject(
				this);
		final JCommanderTranslationMap map = translator.translate();
		map.createFacadeObjects();

		// Copy default parameters over for help display.
		map.transformToFacade();

		JCommander jc = new JCommander();

		final Map<String, TranslationEntry> translations = map.getEntries();
		for (final Object obj : map.getObjects()) {
			for (final Field field : obj.getClass().getDeclaredFields()) {
				final TranslationEntry tEntry = translations.get(
						field.getName());
				if ((tEntry != null) && (tEntry.getObject() instanceof HBaseSetJarPathCommand)) {
					jc.addObject(
							obj);
					break;
				}
			}
		}

		final String programName = StringUtils.join(
				nameArray,
				" ");
		jc.setProgramName(
				programName);
		jc.usage(
				builder);

		// Trim excess newlines.
		final String operations = builder.toString().trim();

		builder = new StringBuilder();
		builder.append(
				operations);
		builder.append(
				"\n\n");
		builder.append(
				"  ");

		jc = new JCommander();

		for (final Object obj : map.getObjects()) {
			for (final Field field : obj.getClass().getDeclaredFields()) {
				final TranslationEntry tEntry = translations.get(
						field.getName());
				if ((tEntry != null) && !(tEntry.getObject() instanceof HBaseSetJarPathCommand)) {
					final Parameters parameters = tEntry.getObject().getClass().getAnnotation(
							Parameters.class);
					if (parameters != null) {
						builder.append(
								parameters.commandDescription());
					}
					else {
						builder.append(
								"Additional Parameters");
					}
					jc.addObject(
							obj);
					break;
				}
			}
		}

		jc.setProgramName(
				programName);
		jc.usage(
				builder);
		builder.append(
				"\n\n");

		return builder.toString().trim();
	}
}
