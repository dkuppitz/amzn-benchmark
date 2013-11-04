/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.loader;

import com.thinkaurelius.amazon.benchmark.GraphLoader;
import com.tinkerpop.blueprints.Graph;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public abstract class VertexLoader<T> implements Runnable {
    
    protected GraphLoader loader;
    protected T entity;
    protected Graph graph;
    protected long batchSize;

    public void init(final GraphLoader loader, final T entity, final long batchSize) {
        this.loader = loader;
        this.graph = loader.getGraph();
        this.entity = entity;
        this.batchSize = batchSize;
    }
}
