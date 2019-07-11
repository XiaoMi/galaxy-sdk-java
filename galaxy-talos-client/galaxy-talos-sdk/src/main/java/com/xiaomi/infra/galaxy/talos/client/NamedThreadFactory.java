/**
 * Copyright 2019, Xiaomi. All rights reserved. Author: zhangqian8@xiaomi.com
 */
package com.xiaomi.infra.galaxy.talos.client;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    private final ThreadGroup group;
    private final String threadName;
    private AtomicInteger threadNumber;
    private boolean addThreadIndexSuffix;

    public NamedThreadFactory(String threadName) {
        this(threadName, true);
    }

    public NamedThreadFactory(String threadName, boolean addThreadIndexSuffix) {
        this.threadNumber = new AtomicInteger(0);
        SecurityManager s = System.getSecurityManager();
        this.group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.threadName = threadName;
        this.addThreadIndexSuffix = addThreadIndexSuffix;
    }

    public Thread newThread(Runnable r) {
        String curThreadName = this.threadName;
        if (this.addThreadIndexSuffix) {
            curThreadName = this.threadName + "-thread-" + this.threadNumber.getAndIncrement();
        }

        Thread t = new Thread(this.group, r, curThreadName, 0L);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }

        if (t.getPriority() != 5) {
            t.setPriority(5);
        }

        return t;
    }
}
