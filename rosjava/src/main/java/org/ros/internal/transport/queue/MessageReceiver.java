/*
 * Copyright (C) 2012 Google Inc.
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

package org.ros.internal.transport.queue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ros.concurrent.CircularBlockingDeque;
import org.ros.internal.transport.tcp.AbstractNamedChannelHandler;
import org.ros.message.MessageDeserializer;

/**
 * @param <T> the message type
 * @author damonkohler@google.com (Damon Kohler)
 */
public class MessageReceiver<T> extends AbstractNamedChannelHandler {

    private static final boolean DEBUG = false;
    private static final Log log = LogFactory.getLog(MessageReceiver.class);

    private final CircularBlockingDeque<LazyMessage<T>> lazyMessages;
    private final MessageDeserializer<T> deserializer;

    public MessageReceiver(CircularBlockingDeque<LazyMessage<T>> lazyMessages,
                           MessageDeserializer<T> deserializer) {
        this.lazyMessages = lazyMessages;
        this.deserializer = deserializer;
    }

    @Override
    public String getName() {
        return "IncomingMessageQueueChannelHandler";
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buffer = (ByteBuf) msg;
        if (DEBUG) {
            log.info(String.format("Received %d byte message.", buffer.readableBytes()));
        }
        // We have to make a defensive copy of the buffer here because Netty does
        // not guarantee that the returned ByteBuf will not be reused.
        lazyMessages.addLast(new LazyMessage<T>(buffer.copy(), deserializer));
        super.channelRead(ctx, msg);
    }

}