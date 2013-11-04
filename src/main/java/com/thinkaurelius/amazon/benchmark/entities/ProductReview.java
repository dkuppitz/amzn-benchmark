/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.entities;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class ProductReview extends AmazonEntity {
    public final User user = new User();
    public Float helpfulness;
    public Float score;
    public Long time;
    public String summary;
    public String text;
}
