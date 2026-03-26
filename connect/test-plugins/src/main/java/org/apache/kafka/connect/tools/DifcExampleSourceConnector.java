/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.tools;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Example source connector that demonstrates how Kafka Connect clients can use
 * DIFC helper methods exposed by {@link org.apache.kafka.connect.connector.Connector}.
 */
public class DifcExampleSourceConnector extends SourceConnector {

    public static final String DIFC_TEST_TAG_CONFIG = "difc.test.tag";
    public static final String DIFC_REQUEST_ADD_CAP_ON_START_CONFIG = "difc.request.add.cap.on.start";
    public static final String DIFC_REQUEST_REMOVE_CAP_ON_START_CONFIG = "difc.request.remove.cap.on.start";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DIFC_TEST_TAG_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Tag used for DIFC sample requests.")
            .define(DIFC_REQUEST_ADD_CAP_ON_START_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "Whether to request CAN_ADD on startup for the configured tag.")
            .define(DIFC_REQUEST_REMOVE_CAP_ON_START_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "Whether to request CAN_REMOVE on startup for the configured tag.");

    private Map<String, String> config;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(final Map<String, String> props) {
        this.config = props;
        registerClient(props);

        final String tag = props.get(DIFC_TEST_TAG_CONFIG);
        if (tag == null || tag.isEmpty()) {
            return;
        }

        createTag(props, tag);
        addTag(props, tag);

        if (Boolean.parseBoolean(props.getOrDefault(DIFC_REQUEST_ADD_CAP_ON_START_CONFIG, "false"))) {
            requestAddCapabilityForTag(props, tag);
        }

        if (Boolean.parseBoolean(props.getOrDefault(DIFC_REQUEST_REMOVE_CAP_ON_START_CONFIG, "false"))) {
            requestRemoveCapabilityForTag(props, tag);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MockSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        return Collections.singletonList(config == null ? Collections.emptyMap() : config);
    }

    @Override
    public void stop() {
        // no-op for example connector
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}

