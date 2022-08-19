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

package org.ros.internal.message;

import io.netty.buffer.ByteBuf;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.StackObjectPool;
import org.ros.exception.RosMessageRuntimeException;

/**
 * A pool of {@link ByteBuf}s for serializing and deserializing messages.
 * <p>
 * By contract, {@link ByteBuf}s provided by {@link #acquire()} must be
 * returned using {@link #release(ByteBuf)}.
 * 
 * @author damonkohler@google.com (Damon Kohler)
 */
public class MessageBufferPool {

  private final ObjectPool<ByteBuf> pool;

  public MessageBufferPool() {
    pool = new StackObjectPool<ByteBuf>(new PoolableObjectFactory<ByteBuf>() {
      @Override
      public ByteBuf makeObject() throws Exception {
        return MessageBuffers.dynamicBuffer();
      }

      @Override
      public void destroyObject(ByteBuf channelBuffer) throws Exception {
      }

      @Override
      public boolean validateObject(ByteBuf channelBuffer) {
        return true;
      }

      @Override
      public void activateObject(ByteBuf channelBuffer) throws Exception {
      }

      @Override
      public void passivateObject(ByteBuf channelBuffer) throws Exception {
        channelBuffer.clear();
      }
    });
  }

  /**
   * Acquired {@link ByteBuf}s must be returned using
   * {@link #release(ByteBuf)}.
   * 
   * @return an unused {@link ByteBuf}
   */
  public ByteBuf acquire() {
    try {
      return pool.borrowObject();
    } catch (Exception e) {
      throw new RosMessageRuntimeException(e);
    }
  }

  /**
   * Release a previously acquired {@link ByteBuf}.
   * 
   * @param channelBuffer
   *          the {@link ByteBuf} to release
   */
  public void release(ByteBuf channelBuffer) {
    try {
      pool.returnObject(channelBuffer);
    } catch (Exception e) {
      throw new RosMessageRuntimeException(e);
    }
  }
}
