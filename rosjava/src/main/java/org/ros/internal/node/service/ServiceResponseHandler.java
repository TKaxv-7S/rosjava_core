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

package org.ros.internal.node.service;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import org.ros.exception.RemoteException;
import org.ros.internal.node.response.StatusCode;
import org.ros.message.MessageDeserializer;
import org.ros.node.service.ServiceResponseListener;

import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

/**
 * A Netty {@link ChannelDuplexHandler} for service responses.
 *
 * @author damonkohler@google.com (Damon Kohler)
 */
class ServiceResponseHandler<ResponseType> extends ChannelDuplexHandler {

    private final Queue<ServiceResponseListener<ResponseType>> responseListeners;
    private final MessageDeserializer<ResponseType> deserializer;
    private final ExecutorService executorService;

    public ServiceResponseHandler(Queue<ServiceResponseListener<ResponseType>> messageListeners,
                                  MessageDeserializer<ResponseType> deserializer, ExecutorService executorService) {
        this.responseListeners = messageListeners;
        this.deserializer = deserializer;
        this.executorService = executorService;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final ServiceResponseListener<ResponseType> listener = responseListeners.poll();
        Preconditions.checkNotNull(listener, "No listener for incoming service response.");
        final ServiceServerResponse response = (ServiceServerResponse) msg;
        final ByteBuf buffer = response.getMessage();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                if (response.getErrorCode() == 1) {
                    listener.onSuccess(deserializer.deserialize(buffer));
                } else {
                    String message = StandardCharsets.US_ASCII.decode(buffer.nioBuffer()).toString();
                    listener.onFailure(new RemoteException(StatusCode.ERROR, message));
                }
            }
        });
    }
}
