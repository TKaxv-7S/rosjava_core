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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

/**
 * Decodes service responses.
 *
 * @author damonkohler@google.com (Damon Kohler)
 */
class ServiceResponseDecoder<ResponseType> extends
        ReplayingDecoder<ServiceResponseDecoderState> {

    private ServiceServerResponse response;

    public ServiceResponseDecoder() {
        reset();
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        switch (state()) {
            case ERROR_CODE:
                response.setErrorCode(byteBuf.readByte());
                checkpoint(ServiceResponseDecoderState.MESSAGE_LENGTH);
            case MESSAGE_LENGTH:
                response.setMessageLength(byteBuf.readInt());
                checkpoint(ServiceResponseDecoderState.MESSAGE);
            case MESSAGE:
                response.setMessage(byteBuf.readBytes(response.getMessageLength()));
                try {
                    return;
                } finally {
                    reset();
                }
            default:
                throw new IllegalStateException();
        }
    }

    private void reset() {
        checkpoint(ServiceResponseDecoderState.ERROR_CODE);
        response = new ServiceServerResponse();
    }
}
