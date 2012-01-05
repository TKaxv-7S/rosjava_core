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

package org.ros.node.topic;

/**
 * A lifecycle listener for {@link Publisher} instances.
 * 
 * @author khughes@google.com (Keith M. Hughes)
 */
public interface PublisherListener<T> extends RegistrantListener<Publisher<T>> {

  /**
   * A {@link Subscriber} has connected to the {@link Publisher}.
   * 
   * @param publisher
   *          the {@link Publisher} which has received the new connection
   */
  void onNewSubscriber(Publisher<T> publisher);

  /**
   * The {@link Publisher} has been shut down.
   * 
   * @param publisher
   *          the {@link Publisher} which has been shut down
   */
  void onShutdown(Publisher<T> publisher);
}