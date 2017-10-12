/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.datastore.hbase.coprocessors;

import java.io.IOException;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import mil.nga.giat.geowave.datastore.hbase.coprocessors.protobuf.SharedDataProtos;

public class SharedDataEndpoint extends
		SharedDataProtos.SharedDataService implements
		Coprocessor,
		CoprocessorService
{
	private static final Logger LOGGER = LoggerFactory.getLogger(SharedDataEndpoint.class);

	private RegionCoprocessorEnvironment env;

	@Override
	public void start(
			final CoprocessorEnvironment env )
			throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		}
		else {
			throw new CoprocessorException(
					"Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(
			final CoprocessorEnvironment env )
			throws IOException {
		// nothing to do when coprocessor is shutting down
	}

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void share(
			final RpcController controller,
			final SharedDataProtos.SharedDataRequest request,
			final RpcCallback<SharedDataProtos.SharedDataResponse> done ) {
		SharedDataProtos.SharedDataResponse response = null;

		String value;

		if (request.hasValue()) {
			value = request.getValue();

			env.getConfiguration().set(
					request.getKey(),
					value);
		}
		else {
			value = env.getConfiguration().get(
					request.getKey());
		}

		response = SharedDataProtos.SharedDataResponse.newBuilder().setValue(
				value).build();

		done.run(response);
	}
}
