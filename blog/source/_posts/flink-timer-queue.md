---
title: Flink实时计算数据落盘方案
date: 2018-11-30 19:33:36
tags: ["flink", "多线程", "背压", "双缓存"]
---


# Flink实时计算数据落盘方案


## 背景

我们使用Flink进行实时统计，并将计算结果更新到Sink中进而实现数据落盘，**为了描述方便，我们以落盘到MySQL为例**。考虑到MySQL读写性能，我们使用双缓存加定时批量Flush的方式更新数据库。在实施过程中碰到了一些问题：

1. Flush时会对数据库的产生极大的压力。
2. 背压，由于使用双缓存，Flink计算的结果只写入内存，无法产生Flush到MySQL时的压力。
3. 数据的完整性，由于在Flush之前，Flink计算的结果还在内存中，当宕机或Task退出都会造成数据丢失。

下面将讨论我们是如何解决上述问题的。



## 数据去重

使用Map进行数据去重，减少MySQL压力。

考虑到Flink计算的大多数操作是统计，如Sum, Count，Max等，在实时流中，每来一条数据都是对结果进行更新，即后来的计算结果会覆盖原来的结果，所以没有必须要每次的计算结果都写到数据库。

我们在内存中进行去重。即使用一个Map缓存，期中Key为聚合的维度，Value是每次计算的结果。每次Flink的计算都put到该Map。后期定时将该缓存Flush到MySQL中。

考虑到维度的基数远小于实时流中消息的数量，将极大缓解数据Flush到MySQL的压力。当然还可以按Task进行分库分表，进一步分摊单个数据库的压力。



## 定时队列

我们使用定时任务将内存中的数据Flush到MySQL中。为此我们定义了一个定时队列。

**主要包括两个数据结构：**

**push buffer：**内部是一个HashMap，用于缓存去重的结果。

**pull buffer：**一个链表，当时需要Flush的数据

**主要实现了三个方法：**

**Swap:** 交换缓存，即将push buffer中的数据交换到pull buffer中，该函数为定时调用。

**push:**  Flink调用该方法将数据push到队列中，如果在一个swap周期内pull buffer中还有数据没有flush到mysql，该方法将阻塞。

**pull：**Flush线程从pull buffer的头部获取数据 并将该数据写到MySQL中，如果pull buffer为空，该方法阻塞。



**主要流程：**

1. Flink会一直push数据到定时队列中，如果存在背压，则push方法阻塞，直到pull buffer中的数据为空。

2. swap时会将当前push buffer中的数据复制到pull buffer中，则清空push buffer，这样push buffer中只会存在一个swap周期内更新过的数据，而这段时间的数据量一般会远小于聚合维度的基数。如果swap时pull buffer还存在数据，可以选择阻塞等待或放弃这些周期。除非特别情况（如：checkpoint），否则可以放弃本次周期，这样并不影响最终结果的正确性。

3. Flush线程或外部任务调用pull方法将拉取数据（一次可以拉取多条，甚至所有的数据），并将拉取后的数据写到MySQL中。如果pull buffer中没有数据，则pull方法阻塞，直到swap方法被调用。



## 结合Checkpoint

定时队列结合Checkpoint，用于实现数据的完整性。

Checkpoint是Flink用于保证完整性的机制，我们需要在Checkpoint中时将定时队列中的数据所有数据都写到MySQL中。流程如下：首先调用时间队列的swap，将push buffer中的数据都交换到pull buffer上，再将pull方法从pull buffer中的获取数据进而写到MySQL中。

**讨论以下分两种情况：**

1. Checkpoint时调用Swap方法，同时pull buffer中没有数据，调用swap可以直接返回，再将pull buffer中的数据写到数据库中
2. Checkpoint时调用Swap方法，同时pull buffer中存在数据，则调用swap时需要让swap阻塞，即而阻塞Checkpoint，直到其他任务先将pull buffer都写到数据，Swap返回继续执行Checkpoint操作。
3. Swap方法保证了线程安全，Checkpoint线程与其他线程无法同时进入该方法。

综上所述，我们只要保证在调用swap方法时，如有背压能阻塞，就能保存数据的完整性。


## 源代码


**PushBuffer**

```java
public interface PushBuffer<T> {
        
    void add(T obj);

    void clear();

    T get(Object tag);

    Iterable<T> values();

    int size();
}

public class TimerQueueMapBuffer<T> implements PushBuffer<T> {

    private final Map<Object, T> map = Maps.newConcurrentMap();

    private final Function<T, Object> keyGetter;

    public TimerQueueMapBuffer(Function<T, Object> keyGetter){
        this.keyGetter = keyGetter;
    }

    @Override
    public void add(T obj) {
        Object key = keyGetter.apply(obj);
        map.put(key, obj);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public T get(Object tag) {
        return map.get(tag);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Iterable<T> values() {
        return map.values();
    }
}


```


**TimerQueue**

```java

public class TimerQueue<T> {

    public final static long WAIT_TIME_MS = 100;

    public final static int PULL_FULL_SIZE = -1;

    public static Logger logger = LoggerFactory.getLogger(TimerQueue.class);

    private final Supplier<PushBuffer<T>> bufferSupplier;
    private final long expiredTime;

    public TimerQueue(Supplier<PushBuffer<T>> bufferSupplier, long expiredTime, boolean autoSwap, Consumer<List<T>> consumer) {

        this.bufferSupplier = bufferSupplier;
        this.expiredTime = expiredTime;

        if (autoSwap) {

            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
                        try {
                            this.swap(true);
                        } catch (Exception ex) {
                            logger.error("failed to swap buffer", ex);
                        }
                    }, 0,
                    expiredTime,
                    TimeUnit.MICROSECONDS);
        }

        if (consumer != null) {
            Executors.newSingleThreadExecutor().submit(() -> this.pub(consumer));
        }

    }

    public TimerQueue(Supplier<PushBuffer<T>> bufferSupplier, long expiredTime) {
        this(bufferSupplier, expiredTime, true, null);
    }

    private volatile long swapTime = -1;

    private volatile PushBuffer<T> pushBuffer;

    private volatile LinkedList<T> pullBuffer = new LinkedList<>();

    private volatile boolean swapping = false;

    private final Lock lock = new ReentrantLock();

    private final Condition pushCondition = lock.newCondition();

    private final Condition pullCondition = lock.newCondition();

    protected boolean hasPressure() {

        boolean pressure = System.currentTimeMillis() - swapTime > expiredTime;

        if (pressure) {
            pressure = getPullSize() > 0;
        }

        return pressure;
    }

    public void push(T obj) {

        if (pushBuffer == null) {

            try {
                lock.lock();
                if (pushBuffer == null) {
                    pushBuffer = bufferSupplier.get();
                }
            } finally {
                lock.unlock();
            }
        }

        try {

            lock.lock();
            while (swapping || hasPressure()) {
                try {
                    pushCondition.await(WAIT_TIME_MS, TimeUnit.MICROSECONDS);
                } catch (Exception ex) {
                    logger.error("await push condition error", ex);
                }
            }

            pushBuffer.add(obj);

        } finally {
            lock.unlock();
        }
    }

    public boolean swap(boolean retryIfFailed) {

        if (retryIfFailed) {
            while (true) {
                try {
                    if (swap()) return true;
                    Thread.sleep(WAIT_TIME_MS);
                } catch (Exception ex) {
                    logger.error("failed to swap buffer", ex);
                }
            }

        } else {
            return swap();
        }
    }

    public boolean swap() {

        try {

            lock.lock();

            if (swapping || getPullSize() > 0) {
                return false;
            }

            swapping = true;

            pullBuffer = new LinkedList<>();

            if (pushBuffer != null) {
                for (T v : pushBuffer.values()) {
                    pullBuffer.add(v);
                }
            }

            this.pushBuffer = bufferSupplier.get();
            this.swapTime = System.currentTimeMillis();

            pullCondition.signalAll();

            return true;

        } finally {
            swapping = false;
            lock.unlock();
        }
    }

    private void pub(Consumer<List<T>> consumer) {

        if (consumer == null) return;

        while (true) {

            try {
                List<T> list = this.pull(PULL_FULL_SIZE, WAIT_TIME_MS);

                if (list == null) {
                    continue;
                }
                consumer.accept(list);
            } catch (Exception ex) {
                logger.error("failed to callback listener", ex);
            }
        }
    }

    public int getPushSize() {

        PushBuffer<T> pushBuffer = this.pushBuffer;
        return pushBuffer == null ? 0 : pushBuffer.size();
    }

    public int getPullSize() {

        LinkedList<T> pullBuffer = this.pullBuffer;
        return pullBuffer == null ? 0 : pullBuffer.size();
    }

    public List<T> pull(int size, long waitTimeMs) {

        try {

            lock.lock();
            
            long waitTime = waitTimeMs == -1 ? WAIT_TIME_MS : waitTimeMs;
            while (getPullSize() == 0) {
                try {
                    pullCondition.await(waitTime, TimeUnit.MICROSECONDS);
                    if (waitTimeMs != -1l) break;

                } catch (Exception ex) {
                    logger.info("exception", ex);
                }
            }

            boolean pressure = hasPressure();

            List<T> list;
            if (size == -1) {
                list = pullBuffer;
                pullBuffer = null;
            } else {

                list = new ArrayList<>();
                for (int i = 0, pullSize = getPullSize(); i < size && i < pullSize; i++) {
                    list.add(pullBuffer.pollFirst());
                }
            }

            if (pressure && getPullSize() == 0) {
                pushCondition.signalAll();
            }

            return list;

        } finally {
            lock.unlock();
        }
    }
}

```



## 问题

1. **在同一个线程中swap后立即pull，可能会造成死锁**

   需要在pull中使用超时参数让阻塞时也能返回。其中 -1，表示一直阻塞。如在checkpoint中，调用如下：

   timerQueue.swap(true);

   timerQueue.pull(oncePullSize, timeWaitMs) ; // 如 timerQueue.pull( 10, 200);

2. 不要一次获取所有数据

   获取所有数据会造成pull buffer数据为空，而将buffer放在本地缓存特后续处理，这样Flink会以为所有的数据都已处理完成，造成无背压的假象，在进行Checkpoint时因无法获取到所有数据而造成数据不完全，同时也会对MySQL造成压力。

3. 目前极端情况下会造成pull出一批（一条）数据但没有及时写入MySQL中。建议保证数据落盘相关代码线程安全。后期考虑添加数据确认机制，即保存数据落盘完成后才真正从pull buffer中删除数据。







