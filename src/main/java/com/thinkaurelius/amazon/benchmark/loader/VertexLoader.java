/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.loader;

import com.tinkerpop.blueprints.Graph;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public abstract class VertexLoader<T> implements Runnable {
    
    protected T entity;
    protected Graph graph;
    protected long batchSize;

    public void init(final Graph graph, final T entity, final long batchSize) {
        this.graph = graph;
        this.entity = entity;
        this.batchSize = batchSize;
    }
}
