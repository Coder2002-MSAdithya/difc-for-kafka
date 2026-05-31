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
package org.apache.kafka.streams.difc;

import org.apache.kafka.common.message.PollPrivsReqResponseData;

/**
 * Callback invoked on the Kafka Streams DIFC polling thread when {@code POLL_PRIVS_REQ}
 * returns a pending capability request for this client (tag owner).
 */
@FunctionalInterface
public interface DifcPrivilegeRequestHandler {

    /**
     * @param pendingRequest response from {@code POLL_PRIVS_REQ}; {@code capability} is {@code -1}
     *                       when the queue is empty (implementations should ignore those).
     */
    void onPrivilegeRequest(PollPrivsReqResponseData pendingRequest);
}
