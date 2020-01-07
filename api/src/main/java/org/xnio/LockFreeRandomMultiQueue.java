package org.xnio;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.openhft.affinity.AffinityLock;
import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by raghuteja on 17/05/19.
 */
public class LockFreeRandomMultiQueue<T> implements BlockingQueue<T> {

  private static final Logger logger = LoggerFactory.getLogger(LockFreeRandomMultiQueue.class);
  private static final int QUEUE_SIZE_LOG_FREQUENCY = 10000;

  private ArrayList<ManyToManyConcurrentArrayQueue<T>> manyToManyConcurrentArrayQueues;
  private Map<Long, Integer> readThreadMap;
  private AtomicInteger readSeq;
  private Map<Long, Integer> writeThreadMap;
  private AtomicInteger writeSeq;
  private int capacity;
  private boolean threadAffinity;


  public LockFreeRandomMultiQueue(int queueCount, boolean threadAffinity, int queueCapacity) {
    this.capacity = queueCount;
    this.threadAffinity = threadAffinity;
    manyToManyConcurrentArrayQueues = new ArrayList<>();
    for (int c = 0; c < capacity; c++) {
      manyToManyConcurrentArrayQueues.add(
          new ManyToManyConcurrentArrayQueue<T>(queueCapacity)
      );
    }
    readThreadMap = new ConcurrentHashMap<>();
    writeThreadMap = new ConcurrentHashMap<>();
    readSeq = new AtomicInteger(0);
    writeSeq = new AtomicInteger(0);
    logQueueSizes();
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
    // this function will be called in case of failure only
    return true;
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
    // this function will be called in case of failure only
    return 0;
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements) {
    throw new UnsupportedOperationException();
  }

  private ManyToManyConcurrentArrayQueue<T> getReadQueue() {
    Long tid = Thread.currentThread().getId();
    Integer index = readThreadMap.get(tid);
    if (index == null) {
      synchronized (readThreadMap) {
        if(!readThreadMap.containsKey(tid)) {
          readThreadMap.put(tid, readSeq.getAndIncrement());
          index = readThreadMap.get(tid);
          acquireAndLogIfRequired(tid, false);
          readSeq.compareAndSet(capacity, 0);
        }
      }
      if(readThreadMap.size() == capacity) {
        readThreadMap = new HashMap<>(readThreadMap);
        System.out.println("Read thread map copied");
      }
    }
    return manyToManyConcurrentArrayQueues.get(index);
  }

  private ManyToManyConcurrentArrayQueue<T> getWriteQueue() {
    return manyToManyConcurrentArrayQueues.get(ThreadLocalRandom.current().nextInt(capacity));
  }

  private void acquireAndLogIfRequired(Long tid, boolean isWrite) {
    Map<Long, Integer> threadMap = isWrite ? writeThreadMap : readThreadMap;
    String mapType = isWrite ? "writeThreadMap" : "readThreadMap";
    if (threadAffinity) {
      AffinityLock affinityLock = AffinityLock.acquireLock(true);
      System.out.println(
          Thread.currentThread().getName() + " : Assigned " + mapType + " thread id : " + tid
              + " : queue id : " + threadMap.get(tid) + " : cpu id : " + affinityLock.cpuId());
    }
    else {
      System.out.println(
          Thread.currentThread().getName() + " : Assigned " + mapType + " thread id : " + tid
              + " : queue id : " + threadMap.get(tid));
    }
  }

  private void logQueueSizes() {
    new Thread(
        new Runnable() {
          @Override
          public void run() {
            while(true) {
              try {
                Thread.sleep(QUEUE_SIZE_LOG_FREQUENCY);
                for(int index = 0; index < manyToManyConcurrentArrayQueues.size(); index++) {
                  System.out.println("Current queue size " + index + " : " +  manyToManyConcurrentArrayQueues.get(index).size());
                }
              } catch (InterruptedException e) {
                System.out.println("QueueSize exception");
              }
            }
          }
        }
    ).start();
  }

}
