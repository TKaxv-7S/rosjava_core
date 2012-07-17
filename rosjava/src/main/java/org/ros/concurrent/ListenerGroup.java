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

package org.ros.concurrent;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A group of listeners.
 * 
 * @author damonkohler@google.com (Damon Kohler)
 */
public class ListenerGroup<T> {

  private final ExecutorService executorService;
  private final Collection<EventDispatcher<T>> eventDispatchers;

  public ListenerGroup(ExecutorService executorService) {
    this.executorService = executorService;
    eventDispatchers = Lists.newCopyOnWriteArrayList();
  }

  /**
   * Adds a listener to the {@link ListenerGroup}.
   * 
   * @param listener
   *          the listener to add
   * @param limit
   *          the maximum number of events to buffer
   * @return the {@link EventDispatcher} responsible for calling the specified
   *         listener
   */
  public EventDispatcher<T> add(T listener, int limit) {
    EventDispatcher<T> eventDispatcher = new EventDispatcher<T>(listener, limit);
    eventDispatchers.add(eventDispatcher);
    executorService.execute(eventDispatcher);
    return eventDispatcher;
  }

  /**
   * Adds the specified listener to the {@link ListenerGroup} with the queue
   * limit set to {@link Integer#MAX_VALUE}.
   * 
   * @param listener
   *          the listener to add
   * @return the {@link EventDispatcher} responsible for calling the specified
   *         listener
   */
  public EventDispatcher<T> add(T listener) {
    return add(listener, Integer.MAX_VALUE);
  }

  /**
   * Adds all the specified listeners to the {@link ListenerGroup}.
   * 
   * @param listeners
   *          the listeners to add
   * @param limit
   *          the maximum number of events to buffer
   * @return a {@link Collection} of {@link EventDispatcher}s responsible for
   *         calling the specified listeners
   */
  public Collection<EventDispatcher<T>> addAll(Collection<T> listeners, int limit) {
    Collection<EventDispatcher<T>> eventDispatchers = Lists.newArrayList();
    for (T listener : listeners) {
      eventDispatchers.add(add(listener, limit));
    }
    return eventDispatchers;
  }

  /**
   * Adds all the specified listeners to the {@link ListenerGroup} with the
   * queue capacity for each set to {@link Integer#MAX_VALUE}.
   * 
   * @param listeners
   *          the listeners to add
   * @return a {@link Collection} of {@link EventDispatcher}s responsible for
   *         calling the specified listeners
   */
  public Collection<EventDispatcher<T>> addAll(Collection<T> listeners) {
    return addAll(listeners, Integer.MAX_VALUE);
  }

  /**
   * @return the number of listeners in the group
   */
  public int size() {
    return eventDispatchers.size();
  }

  /**
   * Signals all listeners.
   * <p>
   * Each {@link SignalRunnable} is executed in a separate thread.
   */
  public void signal(SignalRunnable<T> signalRunnable) {
    for (EventDispatcher<T> eventDispatcher : eventDispatchers) {
      eventDispatcher.signal(signalRunnable);
    }
  }

  /**
   * Signals all listeners and waits for the result.
   * <p>
   * Each {@link SignalRunnable} is executed in a separate thread.
   * 
   * @return {@code true} if all listeners completed within the specified time
   *         limit, {@code false} otherwise
   * @throws InterruptedException
   */
  public boolean signal(SignalRunnable<T> signalRunnable, long timeout, TimeUnit unit)
      throws InterruptedException {
    Collection<ListenableFuture<Void>> futures = Lists.newArrayList();
    for (EventDispatcher<T> eventDispatcher : eventDispatchers) {
      futures.add(eventDispatcher.signal(signalRunnable));
    }
    // We can't use Futures.allAsList() here since it does not provide a
    // timeout.
    final CountDownLatch latch = new CountDownLatch(futures.size());
    for (ListenableFuture<Void> future : futures) {
      future.addListener(new Runnable() {
        @Override
        public void run() {
          latch.countDown();
        }
      }, executorService);
    }
    return latch.await(timeout, unit);
  }

  public void shutdown() {
    for (EventDispatcher<T> eventDispatcher : eventDispatchers) {
      eventDispatcher.cancel();
    }
  }
}