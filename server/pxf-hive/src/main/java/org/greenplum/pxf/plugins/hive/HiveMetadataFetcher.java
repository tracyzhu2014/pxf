package org.greenplum.pxf.plugins.hive;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.model.Metadata;
import org.greenplum.pxf.api.model.MetadataFetcher;
import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.plugins.hive.utilities.ProfileFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class for connecting to Hive's MetaStore and getting schema of Hive tables.
 */
@Component
@RequestScope
public class HiveMetadataFetcher extends HivePlugin implements MetadataFetcher {

    private static final String DELIM_FIELD = "DELIMITER";

    private static final Log LOG = LogFactory.getLog(HiveMetadataFetcher.class);
    private IMetaStoreClient client;
    private JobConf jobConf;
    private final HiveClientWrapper hiveClientWrapper;


    public HiveMetadataFetcher(HiveClientWrapper hiveClientWrapper) {
        this.hiveClientWrapper = hiveClientWrapper;
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();

        // init hive metastore client connection.
        client = hiveClientWrapper.initHiveClient(context, configuration);
        jobConf = new JobConf(configuration);
    }

    /**
     * Fetches metadata of hive tables corresponding to the given pattern
     * For patterns matching more than one table, the unsupported tables are skipped.
     * If the pattern correspond to exactly one table, throws an exception if
     * the table type is not supported or contains unsupported field types.
     * Supported HCatalog types: TINYINT,
     * SMALLINT, INT, BIGINT, BOOLEAN, FLOAT, DOUBLE, STRING, BINARY, TIMESTAMP,
     * DATE, DECIMAL, VARCHAR, CHAR.
     *
     * @param pattern pattern table/file name or pattern in the given source
     */
    @Override
    public List<Metadata> getMetadata(String pattern) throws Exception {

        boolean ignoreErrors = false;
        List<Metadata.Item> tblsDesc = hiveClientWrapper.extractTablesFromPattern(client, pattern);

        if (tblsDesc == null || tblsDesc.isEmpty()) {
            LOG.warn("No tables found for the given pattern: " + pattern);
            return null;
        }

        List<Metadata> metadataList = new ArrayList<>();

        if (tblsDesc.size() > 1) {
            ignoreErrors = true;
        }

        for (Metadata.Item tblDesc : tblsDesc) {
            try {
                Metadata metadata = new Metadata(tblDesc);
                Table tbl = hiveClientWrapper.getHiveTable(client, tblDesc);
                hiveClientWrapper.getSchema(tbl, metadata);
                boolean hasComplexTypes = hiveClientWrapper.hasComplexTypes(metadata);
                metadataList.add(metadata);
                List<Partition> tablePartitions = client.listPartitionsByFilter(tblDesc.getPath(), tblDesc.getName(), "", (short) -1);
                Set<OutputFormat> formats = new HashSet<>();
                //If table has partitions - find out all formats
                for (Partition tablePartition : tablePartitions) {
                    String inputFormat = tablePartition.getSd().getInputFormat();
                    OutputFormat outputFormat = getOutputFormat(inputFormat, hasComplexTypes);
                    formats.add(outputFormat);
                }
                //If table has no partitions - get single format of table
                if (tablePartitions.size() == 0) {
                    String inputFormat = tbl.getSd().getInputFormat();
                    OutputFormat outputFormat = getOutputFormat(inputFormat, hasComplexTypes);
                    formats.add(outputFormat);
                }
                metadata.setOutputFormats(formats);
                Map<String, String> outputParameters = new HashMap<>();
                Integer delimiterCode = hiveClientWrapper.getDelimiterCode(tbl.getSd());
                outputParameters.put(DELIM_FIELD, delimiterCode.toString());
                metadata.setOutputParameters(outputParameters);
            } catch (UnsupportedTypeException | UnsupportedOperationException e) {
                if (ignoreErrors) {
                    LOG.warn("Metadata fetch for " + tblDesc.toString() + " failed. " + e.getMessage());
                } else {
                    throw e;
                }
            }
        }

        return metadataList;
    }

    private OutputFormat getOutputFormat(String inputFormat, boolean hasComplexTypes) throws Exception {
        InputFormat<?, ?> fformat = hiveUtilities.makeInputFormat(inputFormat, jobConf);
        String profile = ProfileFactory.get(fformat, hasComplexTypes);
        String outputFormatClassName = context.getPluginConf().getPlugins(profile).get("OUTPUTFORMAT");
        return OutputFormat.getOutputFormat(outputFormatClassName);
    }

}
