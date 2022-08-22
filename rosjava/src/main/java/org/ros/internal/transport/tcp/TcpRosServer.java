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
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ros.address.AdvertiseAddress;
import org.ros.address.BindAddress;
import org.ros.internal.node.service.ServiceManager;
import org.ros.internal.node.topic.TopicParticipantManager;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The TCP server which is used for data communication between publishers and
 * subscribers or between a service and a service client.
 *
 * <p>
 * This server is used after publishers, subscribers, services and service
 * clients have been told about each other by the master.
 *
 * @author damonkohler@google.com (Damon Kohler)
 */
public class TcpRosServer {

    private static final boolean DEBUG = false;
    private static final Log log = LogFactory.getLog(TcpRosServer.class);

    private final BindAddress bindAddress;
    private final AdvertiseAddress advertiseAddress;
    private final TopicParticipantManager topicParticipantManager;
    private final ServiceManager serviceManager;
    private final ScheduledExecutorService executorService;

    private ChannelFactory<ServerChannel> channelFactory;
    private ServerBootstrap bootstrap;
    private Channel outgoingChannel;
    private ChannelGroup incomingChannelGroup;

    public TcpRosServer(BindAddress bindAddress, AdvertiseAddress advertiseAddress,
                        TopicParticipantManager topicParticipantManager, ServiceManager serviceManager,
                        ScheduledExecutorService executorService) {
        this.bindAddress = bindAddress;
        this.advertiseAddress = advertiseAddress;
        this.topicParticipantManager = topicParticipantManager;
        this.serviceManager = serviceManager;
        this.executorService = executorService;
    }

    public void start() {
        Preconditions.checkState(outgoingChannel == null);
        bootstrap = new ServerBootstrap(new NioEventLoopGroup((Math.max(config.getBossLoopThreadCount(), 2))), new NioEventLoopGroup(config.getEventLoopThreadCount()));
        bootstrap.channel(NioServerSocketChannel.class);
        //已处理
        //bootstrap.option("child.bufferFactory", new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN));
        incomingChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                new TcpServerPipelineFactory(incomingChannelGroup, topicParticipantManager, serviceManager).addPipeline(pipeline);
            }
        });

        ChannelFuture bind = bootstrap.bind(bindAddress.toInetSocketAddress());
        outgoingChannel = bind.channel();
        advertiseAddress.setPortCallable(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return ((InetSocketAddress) outgoingChannel.localAddress()).getPort();
            }
        });
        if (DEBUG) {
            log.info("Bound to: " + bindAddress);
            log.info("Advertising: " + advertiseAddress);
        }
    }

    /**
     * Close all incoming connections and the server socket.
     *
     * <p>
     * Calling this method more than once has no effect.
     */
    public void shutdown() {
        if (DEBUG) {
            log.info("Shutting down: " + getAddress());
        }
        if (outgoingChannel != null) {
            outgoingChannel.close().awaitUninterruptibly();
        }
        incomingChannelGroup.close().awaitUninterruptibly();
        // NOTE(damonkohler): We are purposely not calling
        // channelFactory.releaseExternalResources() or
        // bootstrap.releaseExternalResources() since only external resources are
        // the ExecutorService and control of that must remain with the overall
        // application.
        outgoingChannel = null;
    }

    /**
     * @return the advertise-able {@link InetSocketAddress} of this
     * {@link TcpRosServer}
     */
    public InetSocketAddress getAddress() {
        return advertiseAddress.toInetSocketAddress();
    }

    /**
     * @return the {@link AdvertiseAddress} of this {@link TcpRosServer}
     */
    public AdvertiseAddress getAdvertiseAddress() {
        return advertiseAddress;
    }
}
