/*
 * Copyright (C) 2011 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.ros.internal.transport.tcp;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import org.ros.internal.transport.ConnectionTrackingHandler;

/**
 * @author damonkohler@google.com (Damon Kohler)
 */
public class ConnectionTrackingChannelPipelineFactory {

    public static final String CONNECTION_TRACKING_HANDLER = "ConnectionTrackingHandler";

    private final ConnectionTrackingHandler connectionTrackingHandler;

    public ConnectionTrackingChannelPipelineFactory(ChannelGroup channelGroup) {
        this.connectionTrackingHandler = new ConnectionTrackingHandler(channelGroup);
    }

    public void addPipeline(ChannelPipeline channelPipeline) {
        channelPipeline.addLast(CONNECTION_TRACKING_HANDLER, connectionTrackingHandler);
    }
}
