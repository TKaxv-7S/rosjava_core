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
import io.netty.buffer.ByteBufAllocator;

import java.nio.ByteOrder;

/**
 * Provides {@link ByteBuf}s for serializing and deserializing messages.
 *
 * @author damonkohler@google.com (Damon Kohler)
 */
public class MessageBuffers {

  static final int ESTIMATED_LENGTH = 256;

  private MessageBuffers() {
    // Utility class.
  }

  /**
   * @return a new {@link ByteBuf} for {@link Message} serialization that
   *         grows dynamically
   */
  public static ByteBuf dynamicBuffer() {
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(ESTIMATED_LENGTH);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    return buffer;
  }
}
