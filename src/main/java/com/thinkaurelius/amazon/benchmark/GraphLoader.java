/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark;

import com.thinkaurelius.amazon.benchmark.entities.*;
import com.thinkaurelius.amazon.benchmark.input.*;
import com.thinkaurelius.amazon.benchmark.loader.*;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public abstract class GraphLoader<T extends Graph> implements Closeable {

    private final static Logger logger = Logger.getLogger(GraphLoader.class.getName());
    private final static Integer NUM_CPUS = Runtime.getRuntime().availableProcessors();
    
    private final ExecutorService executor;
    private final String productASINFilename;
    private final String userIdFilename;
    private final String categoryFilename;
    private final String productTitleFilename;
    private final String productReviewFilename;
    private final String categoryLinkFilename;
    private final Boolean compact;
    private final Long batchSize;
    private final Boolean multithreaded;

    protected final Map<String, Object> settings;
    protected T graph;

    public GraphLoader(final String[] args, final String graphDbName) throws Exception {

        final ArgumentParser parser = ArgumentParsers.newArgumentParser(this.getClass().getName())
            .defaultHelp(true)
            .description(String.format("Imports Amazon dataset into a %s.", graphDbName));

        this.settings = new HashMap<>();
        try {
            this.configureArgumentParser(parser);
            parser.parseArgs(args, this.settings);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        this.executor = Executors.newFixedThreadPool(NUM_CPUS);
        this.graph = this.createGraphInstance();
        this.multithreaded = this.graph.getFeatures().supportsThreadedTransactions;
        this.productASINFilename = (String)this.settings.get("asin");
        this.userIdFilename = (String)this.settings.get("users");
        this.categoryFilename = (String)this.settings.get("categories");
        this.productReviewFilename = (String)this.settings.get("reviews");
        this.productTitleFilename = (String)this.settings.get("titles");
        this.categoryLinkFilename = (String)this.settings.get("categoryLinks");
        this.compact = (Boolean)this.settings.get("compact");
        this.batchSize = (Long)this.settings.get("batchSize");
    }

    @Override
    public void close() {
        this.graph.shutdown();
    }
    
    protected void configureArgumentParser(final ArgumentParser parser) {
        parser.addArgument("-a", "--asin").help("product ASIN file").required(true).type(String.class);
        parser.addArgument("-u", "--users").help("user ID file").required(true).type(String.class);
        parser.addArgument("-c", "--categories").help("category paths file").required(true).type(String.class);
        parser.addArgument("-t", "--titles").help("product title file").required(false).type(String.class);
        parser.addArgument("-r", "--reviews").help("product reviews file").required(true).type(String.class);
        parser.addArgument("-l", "--categoryLinks").help("category links file").required(false).type(String.class);
        parser.addArgument("-b", "--batchSize").help("batch size for transactional graphs").required(false).setDefault(10000L).type(Long.class);
        parser.addArgument("--compact").help("whether to load all data (false) or only non-textual data (true)").nargs("?").setConst(true).setDefault(false);
    }
    
    protected abstract T createGraphInstance() throws Exception;
    protected abstract void init();

    protected void load() throws IOException, InterruptedException {
        
        this.init();

        final long startTime = System.currentTimeMillis();
        
        if (this.multithreaded) {
            this.loadMultiThreaded();
        }
        else {
            this.loadASINs();
            this.loadUserIds();
            this.loadCategoryPaths();
            this.loadReviews(compact);
            this.loadTitles(compact);
            this.loadCategoryLinks();
        }

        logger.log(Level.INFO, "LOAD TIME :: {0} ms", (System.currentTimeMillis() - startTime));
    }
    
    protected void loadMultiThreaded() {

        final GraphLoader loader = this;
        final CountDownLatch asinLatch = new CountDownLatch(1);
        final CountDownLatch userLatch = new CountDownLatch(1);
        final CountDownLatch pathLatch = new CountDownLatch(1);
        final CountDownLatch all = new CountDownLatch(6);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setPriority(Thread.NORM_PRIORITY + 1);
                loader.loadASINs();
                asinLatch.countDown();
                all.countDown();
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setPriority(Thread.NORM_PRIORITY + 2);
                loader.loadUserIds();
                userLatch.countDown();
                all.countDown();
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                loader.loadCategoryPaths();
                pathLatch.countDown();
                all.countDown();
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    asinLatch.await();
                    userLatch.await();
                    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                    loader.loadReviews(compact);
                }
                catch (InterruptedException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
                finally {
                    all.countDown();
                }
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    asinLatch.await();
                    loader.loadTitles(compact);
                }
                catch (InterruptedException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
                finally {
                    all.countDown();
                }
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    asinLatch.await();
                    pathLatch.await();
                    Thread.currentThread().setPriority(Thread.NORM_PRIORITY + 1);
                    loader.loadCategoryLinks();
                }
                catch (InterruptedException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
                finally {
                    all.countDown();
                }
            }
        });

        try {
            all.await();
        } catch (InterruptedException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    private void loadASINs() {
        try {
            final KeyFileReader asins = new KeyFileReader(productASINFilename);
            this.load(asins, new KeyLoader(Schema.Keys.PRODUCT_ASIN));
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    private void loadUserIds() {
        try {
            final KeyFileReader userIds = new KeyFileReader(userIdFilename);
            this.load(userIds, new KeyLoader(Schema.Keys.USER_ID));
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    private void loadCategoryPaths() {
        try {
            final KeyFileReader paths = new KeyFileReader(categoryFilename);
            this.load(paths, new CategoryPathLoader());
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    private void loadReviews(final boolean compact) {

        try (ProductReviewFileReader productReviews = new ProductReviewFileReader(productReviewFilename, compact)) {
            this.load(productReviews, new ProductReviewLoader());
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    private void loadTitles(final boolean compact) {

        try (ProductTitleFileReader productTitles = new ProductTitleFileReader(productTitleFilename, compact)) {
            this.load(productTitles, new ProductTitleLoader());
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    private void loadCategoryLinks() {

        try (CategoryLinkFileReader categories = new CategoryLinkFileReader(categoryLinkFilename)) {
            this.load(categories, new CategoryLinkLoader());
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    private <T> void load(final Iterable<T> reader, final VertexLoader loader) {

        Graph g = this.getNextTransactionGraph();
        long counter = 0L;

        final Iterator<T> itty = reader.iterator();
        while (itty.hasNext()) {
            final T item = itty.next();
            if (!(item instanceof AmazonEntity) || ((AmazonEntity)item).isValid()) {
                loader.init(g, item, batchSize);
                loader.run();
                if (++counter%batchSize == 0L) {
                    this.commit(g);
                    g = this.getNextTransactionGraph();
                }
            }
        }

        this.commit(g);
    }

    private void commit(final Graph g) {
        if (g instanceof TransactionalGraph) {
            ((TransactionalGraph)g).commit();
        }
    }

    private Graph getNextTransactionGraph() {
        if (this.multithreaded) {
            return ((ThreadedTransactionalGraph)this.graph).newTransaction();
        }
        return this.graph;
    }
}
