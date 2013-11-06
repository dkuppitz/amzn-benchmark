/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.loader;

import com.tinkerpop.blueprints.Vertex;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class KeyLoader extends VertexLoader<String> {

    private final static Logger logger = Logger.getLogger(KeyLoader.class.getName());
    private final static ConcurrentMap<String, AtomicLong> counterMap = new ConcurrentHashMap<>();
    
    private final String key;
    private final String message;
    private final AtomicLong counter;
    
    public KeyLoader(final String key) {
        this.key = key;
        this.message = String.format("%s :: {0}", key.toUpperCase());
        counterMap.putIfAbsent(key, new AtomicLong(0L));
        this.counter = counterMap.get(key);
    }

    @Override
    public void run() {
        if (!"unknown".equals(this.entity)) {
            final Vertex v = this.graph.addVertex(null);
            v.setProperty(key, this.entity);
            if (counter.incrementAndGet()%batchSize == 0L) {
                logger.log(Level.INFO, this.message, counter.get());
            }
        }
    }
}
