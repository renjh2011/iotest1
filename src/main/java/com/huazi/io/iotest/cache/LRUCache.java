package com.huazi.io.iotest.cache;


import com.huazi.io.iotest.btree.Btree;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private int capacity;
    ExecutorService executorService;
    public LRUCache(int capacity) {
        super(capacity, 0.75F, true);
        this.capacity = capacity;
        ExecutorService executorService = new ScheduledThreadPoolExecutor(1);
//        executorService.submit();
    }

    @Override
    public V get(Object key) {
        return super.get(key);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
        if(size() > capacity){
            /*ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2,2,100,TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(10));
            threadPoolExecutor.submit(()->{
                BTree.BTreeNode node = (BTree.BTreeNode) eldest.getValue();
                try {
                    node.write();
                    remove(eldest.getKey());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });*/
            Btree.BTreeNode node = (Btree.BTreeNode) eldest.getValue();
            try {
                node.write();
                return true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public V put(K key, V value) {
        return super.put(key, value);
    }
}