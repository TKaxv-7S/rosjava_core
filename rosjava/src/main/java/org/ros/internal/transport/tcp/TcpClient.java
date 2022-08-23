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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.ros.exception.RosRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author damonkohler@google.com (Damon Kohler)
 */
public class TcpClient {

    private static final boolean DEBUG = false;
    private static final Logger log = LoggerFactory.getLogger(TcpClient.class);

    private static final int DEFAULT_CONNECTION_TIMEOUT_DURATION = 5;
    private static final TimeUnit DEFAULT_CONNECTION_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final boolean DEFAULT_KEEP_ALIVE = true;

    private final ChannelGroup channelGroup;
    private final Bootstrap bootstrap;
    private final List<NamedChannelHandler> namedChannelHandlers;

    private Channel channel;

    public TcpClient(final ChannelGroup channelGroup, final Executor executor) {
        this.channelGroup = channelGroup;
        //channelFactory = new channelFactory(executor, executor);
        //已处理
        //channelBufferFactory = new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN);
        bootstrap = new Bootstrap();
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.group(new NioEventLoopGroup(2, executor));
        //bootstrap.channelFactory(channelFactory);
//    bootstrap = new ClientBootstrap(channelFactory);
        //bootstrap.option("bufferFactory", channelBufferFactory);
        setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT_DURATION, DEFAULT_CONNECTION_TIMEOUT_UNIT);
        setKeepAlive(DEFAULT_KEEP_ALIVE);
        namedChannelHandlers = Lists.newArrayList();
    }

    public void setConnectionTimeout(final int duration, final TimeUnit unit) {
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) unit.toMillis(duration));
    }

    public void setKeepAlive(final boolean value) {
        bootstrap.option(ChannelOption.SO_KEEPALIVE, value);
    }

    public void addNamedChannelHandler(final NamedChannelHandler namedChannelHandler) {
        namedChannelHandlers.add(namedChannelHandler);
    }

    public void addAllNamedChannelHandlers(final List<NamedChannelHandler> namedChannelHandlers) {
        this.namedChannelHandlers.addAll(namedChannelHandlers);
    }

    public void connect(final String connectionName, final SocketAddress socketAddress) {
        TcpClientPipelineFactory tcpClientPipelineFactory = new TcpClientPipelineFactory(channelGroup);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                tcpClientPipelineFactory.getPipeline(pipeline);
                for (final NamedChannelHandler namedChannelHandler : namedChannelHandlers) {
                    pipeline.addLast(namedChannelHandler.getName(), namedChannelHandler);
                }
            }
        });
        final ChannelFuture future = bootstrap.connect(socketAddress).awaitUninterruptibly();
        if (future.isSuccess()) {
            channel = future.channel();
            if (DEBUG) {
                log.info("Connected to: " + socketAddress);
            }
        } else {
            // We expect the first connection to succeed. If not, fail fast.
            throw new RosRuntimeException("Connection exception: " + socketAddress, future.cause());
        }
    }

    public Channel getChannel() {
        return channel;
    }

    public ChannelFuture write(final ByteBuf buffer) {
        Preconditions.checkNotNull(channel);
        Preconditions.checkNotNull(buffer);
        return channel.write(buffer);
    }

    /**
     * Close all incoming connections and the server socket.
     *
     * <p>
     * Calling this method more than once has no effect.
     */
    public void shutdown() {
        log.info("Ros Client Shutting down: " + channel.localAddress());
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        BootstrapConfig config = bootstrap.config();
        //关闭主线程组
        config.group().shutdownGracefully();
    }
}