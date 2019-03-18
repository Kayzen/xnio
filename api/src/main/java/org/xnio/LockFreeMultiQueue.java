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

  private static AtomicInteger seq = new AtomicInteger(0);

  private ArrayList<OneToOneConcurrentArrayQueue<T> > oneToOneConcurrentArrayQueues;
  private final ConcurrentHashMap<Long, Integer> readThreadMap;
  private AtomicInteger readSeq;
  private final ConcurrentHashMap<Long, Integer> writeThreadMap;
  private AtomicInteger writeSeq;

  private int seqID;

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
    seqID = seq.incrementAndGet();
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
    System.out.println("UnImplemented Function : put");
  }

  @Override
  public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
    System.out.println("UnImplemented Function : offer");
    return false;
  }

  @Override
  public T take() throws InterruptedException {
    return poll();
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    System.out.println("UnImplemented Function : poll");
    return null;
  }

  @Override
  public int remainingCapacity() {
    System.out.println("UnImplemented Function : remainingCapacity");
    return 0;
  }

  @Override
  public boolean remove(Object o) {
    System.out.println("UnImplemented Function : remove(o)");
    return false;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    System.out.println("UnImplemented Function : containsAll");
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    System.out.println("UnImplemented Function : addAll");
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    System.out.println("UnImplemented Function : removeAll");
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    System.out.println("UnImplemented Function : retainAll");
    return false;
  }

  @Override
  public void clear() {
    System.out.println("UnImplemented Function : clear");
  }

  @Override
  public int size() {
    System.out.println("UnImplemented Function : size");
    return 0;
  }

  @Override
  public boolean isEmpty() {
    System.out.println("UnImplemented Function : isEmpty");
    return false;
  }

  @Override
  public boolean contains(Object o) {
    System.out.println("UnImplemented Function : contains");
    return false;
  }

  @Override
  public Iterator<T> iterator() {
    System.out.println("UnImplemented Function : iterator");
    return null;
  }

  @Override
  public Object[] toArray() {
    System.out.println("UnImplemented Function : toArray()");
    return new Object[0];
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    System.out.println("UnImplemented Function : toArray(a)");
    return null;
  }

  @Override
  public int drainTo(Collection<? super T> c) {
    System.out.println("UnImplemented Function : drainTo(c)");
    return 0;
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements) {
    System.out.println("UnImplemented Function : drainTo(c, m");
    return 0;
  }

  private OneToOneConcurrentArrayQueue<T> getReadQueue() {
    Long tid = Thread.currentThread().getId();
    if (!readThreadMap.contains(tid)) {
      synchronized (readThreadMap) {
        if(!readThreadMap.contains(tid)) {
          readThreadMap.put(tid, readSeq.getAndIncrement());
          System.out.println(seqID + " assigned readThreadMap " + tid + " : " + readThreadMap.get(tid));
        }
      }
    }
    return oneToOneConcurrentArrayQueues.get(
        readThreadMap.get(tid)
    );
  }

  private OneToOneConcurrentArrayQueue<T> getWriteQueue() {
    Long tid = Thread.currentThread().getId();
    if (!writeThreadMap.contains(tid)) {
      synchronized (writeThreadMap) {
        if(!writeThreadMap.contains(tid)) {
          writeThreadMap.put(tid, writeSeq.getAndIncrement());
          System.out.println(seqID + " assigned writeThreadMap " + tid + " : " + writeThreadMap.get(tid));
        }
      }
    }
    return oneToOneConcurrentArrayQueues.get(
        writeThreadMap.get(tid)
    );
  }
}
