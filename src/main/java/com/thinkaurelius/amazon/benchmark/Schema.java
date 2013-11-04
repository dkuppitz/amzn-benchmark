/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class Schema {
    
    public static class Keys {
        public static final String PRODUCT_ASIN = "ASIN";
        public static final String PRODUCT_TITLE = "title";
        public static final String CATEGORY_NAME = "name";
        public static final String USER_ID = "userId";
        public static final String USER_NAME = "profileName";
        public static final String REVIEW_TIME = "time";
        public static final String REVIEW_SCORE = "score";
        public static final String REVIEW_HELPFULNESS = "helpfulness";
        public static final String REVIEW_SUMMARY = "summary";
        public static final String REVIEW_TEXT = "text";
    }
    
    public static class Labels {
        public static final String HAS_CATEGORY = "hasCategory";
        public static final String HAS_PRODUCT = "hasProduct";
        public static final String REVIEWED = "reviewed";
    }
}
