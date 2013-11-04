/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.input;

import com.thinkaurelius.amazon.benchmark.entities.ProductReview;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class ProductReviewFileReader extends InputFileReader<ProductReview> {
    
    private final boolean compact;

    public ProductReviewFileReader(final String filename, final boolean compact) throws FileNotFoundException, IOException {
        super(filename);
        this.compact = compact;
    }

    @Override
    public ProductReview next() {
        if (this.hasNext()) {
            final ProductReview review = new ProductReview();
            while (true) {
                final String[] keyValue = this.nextLine.split(": ", 2);
                if (keyValue.length == 2) {
                    switch (keyValue[0]) {
                        case "product/productId":
                            review.ASIN = keyValue[1];
                            break;
                        case "review/userId":
                            review.user.userId = keyValue[1];
                            break;
                        case "review/profileName":
                            review.user.profileName = keyValue[1];
                            break;
                        case "review/helpfulness":
                            final String[] tuple = keyValue[1].split("/");
                            final int x = Integer.parseInt(tuple[0]);
                            final int y = Integer.parseInt(tuple[1]);
                            if (y > 0) {
                                review.helpfulness = (float)x / (float)y;
                            }
                            break;
                        case "review/score":
                            review.score = -Float.parseFloat(keyValue[1]);
                            break;
                        case "review/time":
                            review.time = Long.parseLong(keyValue[1]);
                            break;
                        case "review/summary":
                            if (!compact) {
                                review.summary = keyValue[1];
                            }
                            break;
                        case "review/text":
                            if (!compact) {
                                review.text = keyValue[1];
                            }
                            break;
                        default:
                            if (review.text != null) {
                                review.text = review.text + this.nextLine.trim();
                            }
                    }
                    this.readNextLine();
                    if (this.nextLine == null) {
                        break;
                    }
                }
                else {
                    while (this.nextLine.length() > 0) {
                        if (review.text != null) {
                            review.text = review.text + this.nextLine.trim();
                        }
                        this.readNextLine();
                    }
                    hasRead = false;
                    break;
                }
            }
            return review;
        }
        else {
            throw new RuntimeException("Cannot call next() on empty iterator.");
        }
    }
}
