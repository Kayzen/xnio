package org.xnio;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

/**
 * Created by raghuteja on 15/03/19.
 */
public class LockFreeMultiQueue<T> implements BlockingQueue<T> {

  private ArrayList<OneToOneConcurrentArrayQueue<T> > oneToOneConcurrentArrayQueues;
  private final ConcurrentHashMap<Long, Integer> readThreadMap;
  private AtomicInteger readSeq;
  private final ConcurrentHashMap<Long, Integer> writeThreadMap;
  private AtomicInteger writeSeq;

  public LockFreeMultiQueue(int capacity) {
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
    if (!readThreadMap.containsKey(tid)) {
      synchronized (readThreadMap) {
        if(!readThreadMap.containsKey(tid)) {
          readThreadMap.put(tid, readSeq.getAndIncrement());
          System.out.println("Assigned readThreadMap " + tid + " : " + readThreadMap.get(tid));
        }
      }
    }
    return oneToOneConcurrentArrayQueues.get(
        readThreadMap.get(tid)
    );
  }

  private OneToOneConcurrentArrayQueue<T> getWriteQueue() {
    Long tid = Thread.currentThread().getId();
    if (!writeThreadMap.containsKey(tid)) {
      synchronized (writeThreadMap) {
        if(!writeThreadMap.containsKey(tid)) {
          writeThreadMap.put(tid, writeSeq.getAndIncrement());
          System.out.println("Assigned writeThreadMap " + tid + " : " + writeThreadMap.get(tid));
        }
      }
    }
    return oneToOneConcurrentArrayQueues.get(
        writeThreadMap.get(tid)
    );
  }
}
