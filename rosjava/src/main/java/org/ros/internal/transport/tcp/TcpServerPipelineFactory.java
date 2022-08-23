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
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.ros.internal.node.service.ServiceManager;
import org.ros.internal.node.topic.TopicParticipantManager;

import java.nio.ByteOrder;

/**
 * @author damonkohler@google.com (Damon Kohler)
 */
public class TcpServerPipelineFactory extends ConnectionTrackingChannelPipelineFactory {

    public static final String LENGTH_FIELD_BASED_FRAME_DECODER = "LengthFieldBasedFrameDecoder";
    public static final String LENGTH_FIELD_PREPENDER = "LengthFieldPrepender";
    public static final String HANDSHAKE_HANDLER = "HandshakeHandler";

    private final TopicParticipantManager topicParticipantManager;
    private final ServiceManager serviceManager;

    public TcpServerPipelineFactory(ChannelGroup channelGroup, TopicParticipantManager topicParticipantManager, ServiceManager serviceManager) {
        super(channelGroup);
        this.topicParticipantManager = topicParticipantManager;
        this.serviceManager = serviceManager;
    }

    @Override
    public void getPipeline(ChannelPipeline channelPipeline) {
        super.getPipeline(channelPipeline);
        channelPipeline.addLast(LENGTH_FIELD_PREPENDER, new LengthFieldPrepender(4));
        channelPipeline.addLast(LENGTH_FIELD_BASED_FRAME_DECODER, new LengthFieldBasedFrameDecoder(
                ByteOrder.LITTLE_ENDIAN, Integer.MAX_VALUE, 0, 4, 0, 4, true));
        channelPipeline.addLast(HANDSHAKE_HANDLER, new TcpServerHandshakeHandler(topicParticipantManager,
                serviceManager));
    }
}
