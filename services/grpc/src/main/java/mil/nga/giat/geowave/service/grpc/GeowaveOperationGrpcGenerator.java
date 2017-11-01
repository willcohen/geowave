package mil.nga.giat.geowave.service.grpc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.logging.Level;

import org.reflections.Reflections;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;

public class GeowaveOperationGrpcGenerator
{
	public static void main(
			final String[] args ) {
		System.out.print("Parsing commands");
	}

	/**
	 * This method parses all the Geowave Operation classes and creates the info
	 * to generate a Restlet route based on the operation. These routes are
	 * stored in the corresponding member variables including those that are
	 * unavailable
	 */
	public void parseOperationsForApiRoutes() {
		// availableRoutes = new ArrayList<RestRoute>();
		// unavailableCommands = new ArrayList<String>();

		/*
		 * for (final Class<?> operation : new Reflections(
		 * "mil.nga.giat.geowave"
		 * ).getTypesAnnotatedWith(GeowaveOperation.class)) { if
		 * ((operation.getAnnotation( GeowaveOperation.class).restEnabled() ==
		 * GeowaveOperation.RestEnabledType.GET) || (((operation.getAnnotation(
		 * GeowaveOperation.class).restEnabled() ==
		 * GeowaveOperation.RestEnabledType.POST)) && DefaultOperation.class
		 * .isAssignableFrom(operation))) {
		 * 
		 * // availableRoutes.add(new RestRoute( // operation)); } else { final
		 * GeowaveOperation operationInfo =
		 * operation.getAnnotation(GeowaveOperation.class); //
		 * unavailableCommands.add(operation.getName() + " " + //
		 * operationInfo.name()); } }
		 */
	}

}
