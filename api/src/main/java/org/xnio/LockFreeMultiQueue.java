package org.xnio;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.openhft.affinity.AffinityLock;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by raghuteja on 15/03/19.
 */
public class LockFreeMultiQueue<T> implements BlockingQueue<T> {

  private static final Logger logger = LoggerFactory.getLogger(LockFreeMultiQueue.class);

  private ArrayList<OneToOneConcurrentArrayQueue<T> > oneToOneConcurrentArrayQueues;
  private Map<Long, Integer> readThreadMap;
  private AtomicInteger readSeq;
  private Map<Long, Integer> writeThreadMap;
  private AtomicInteger writeSeq;
  private int capacity;
  private boolean threadAffinity;

  public LockFreeMultiQueue(int capacity, boolean threadAffinity) {
    this.capacity = capacity;
    this.threadAffinity = threadAffinity;
    oneToOneConcurrentArrayQueues = new ArrayList<>();
    for (int c = 0; c < capacity; c++) {
      oneToOneConcurrentArrayQueues.add(
          new OneToOneConcurrentArrayQueue<T>(10000)
      );
    }
    readThreadMap = new ConcurrentHashMap<>();
    writeThreadMap = new ConcurrentHashMap<>();
    readSeq = new AtomicInteger(0);
    writeSeq = new AtomicInteger(0);
  }

  @Override
  public boolean add(T t) {
    return offer(t);
  }

  @Override
  public boolean offer(T t) {
    return getWriteQueue().offer(t);
  }

  @Override
  public T remove() {
    return poll();
  }

  @Override
  public T poll() {
    return getReadQueue().poll();
  }

  @Override
  public T element() {
    return peek();
  }

  @Override
  public T peek() {
    return getReadQueue().peek();
  }

  @Override
  public void put(T t) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public T take() throws InterruptedException {
    return poll();
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int drainTo(Collection<? super T> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements) {
    throw new UnsupportedOperationException();
  }

  private OneToOneConcurrentArrayQueue<T> getReadQueue() {
    Long tid = Thread.currentThread().getId();
    Integer index = readThreadMap.get(tid);
    if (index == null) {
      synchronized (readThreadMap) {
        if(!readThreadMap.containsKey(tid)) {
          readThreadMap.put(tid, readSeq.getAndIncrement());
          index = readThreadMap.get(tid);
          acquireAndLogIfRequired(tid, readThreadMap);
        }
      }
      if(readThreadMap.size() == capacity) {
        readThreadMap = new HashMap<>(readThreadMap);
        logger.info("Read thread map copied");
      }
    }
    return oneToOneConcurrentArrayQueues.get(index);
  }

  private OneToOneConcurrentArrayQueue<T> getWriteQueue() {
    Long tid = Thread.currentThread().getId();
    Integer index = writeThreadMap.get(tid);
    if (index == null) {
      synchronized (writeThreadMap) {
        if(!writeThreadMap.containsKey(tid)) {
          writeThreadMap.put(tid, writeSeq.getAndIncrement());
          index = writeThreadMap.get(tid);
          acquireAndLogIfRequired(tid, writeThreadMap);
        }
      }
      if(writeThreadMap.size() == capacity) {
        writeThreadMap = new HashMap<>(writeThreadMap);
        logger.info("Write thread map copied");
      }
    }
    return oneToOneConcurrentArrayQueues.get(index);
  }

  private void acquireAndLogIfRequired(Long tid, Map<Long, Integer> threadMap) {
    if (threadAffinity) {
      AffinityLock affinityLock = AffinityLock.acquireCore(true);
      logger.info(
          Thread.currentThread().getName() + " : Assigned readThreadMap thread id : " + tid
              + " : queue id : " + threadMap.get(tid) + " : cpu id : " + affinityLock.cpuId());
    }
    else {
      logger.info(
          Thread.currentThread().getName() + " : Assigned readThreadMap thread id : " + tid
              + " : queue id : " + threadMap.get(tid));
    }
  }
}
