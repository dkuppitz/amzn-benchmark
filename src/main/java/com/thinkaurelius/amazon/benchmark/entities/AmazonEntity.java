/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.entities;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class AmazonEntity {
    
    public String ASIN;

    public boolean isValid() {
        return this.ASIN != null && this.ASIN.matches("^[A-Z0-9]{10}$");
    }
}
