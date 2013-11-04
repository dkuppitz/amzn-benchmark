/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark;

import com.thinkaurelius.amazon.benchmark.entities.*;
import com.thinkaurelius.amazon.benchmark.input.*;
import com.thinkaurelius.amazon.benchmark.loader.*;
import com.tinkerpop.blueprints.Graph;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
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
    
    private final static String SKIP_MSG = "Skipping %s. The input file '%s' does not exist.";

    private final static int NUM_CPUS = Runtime.getRuntime().availableProcessors();
    private final static int MAX_PARALLEL_TASKS = NUM_CPUS << 2;

    private final static Semaphore semaphore = new Semaphore(MAX_PARALLEL_TASKS, true);

    private final AtomicLong counter = new AtomicLong(0L);
    private final ExecutorService executor;
    private final String productASINFilename;
    private final String userIdFilename;
    private final String categoryFilename;
    private final String productTitleFilename;
    private final String productReviewFilename;
    private final String categoryLinkFilename;
    private final Boolean compact;
    private final Long batchSize;

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

    protected void commit() {
    }

    protected void load() throws IOException, InterruptedException {
        
        this.init();

        final long startTime = System.currentTimeMillis();
        final boolean multithreaded = this.graph.getFeatures().supportsThreadedTransactions;

        this.loadASINs(multithreaded);
        this.loadUserIds(multithreaded);
        this.loadCategoryPaths();
        this.commit();

        if (this.productReviewFilename != null) {
            try { this.loadReviews(compact, multithreaded); }
            catch (FileNotFoundException ex) {
                logger.log(Level.WARNING, String.format(SKIP_MSG, "product reviews", this.productReviewFilename));
            }
        }
        
        if (this.productTitleFilename != null) {
            try { this.loadTitles(compact, multithreaded); }
            catch (FileNotFoundException ex) {
                logger.log(Level.WARNING, String.format(SKIP_MSG, "product titles", this.productTitleFilename));
            }
        }

        if (this.categoryLinkFilename != null) {
            try { this.loadCategoryLinks(multithreaded); }
            catch (FileNotFoundException ex) {
                logger.log(Level.WARNING, String.format(SKIP_MSG, "category links", this.categoryLinkFilename));
            }
        }
        
        if (multithreaded) {
            waitForAllTasks();
        }
        
        this.commit();

        logger.log(Level.INFO, "LOAD TIME :: {0} ms", (System.currentTimeMillis() - startTime));
    }
    
    private void loadASINs(final boolean multithreaded)
            throws FileNotFoundException, InterruptedException, IOException {

        final KeyFileReader asins = new KeyFileReader(productASINFilename);
        this.load(asins, new KeyLoader(Schema.Keys.PRODUCT_ASIN), multithreaded);
    }
    
    private void loadUserIds(final boolean multithreaded)
            throws FileNotFoundException, InterruptedException, IOException {

        final KeyFileReader userIds = new KeyFileReader(userIdFilename);
        this.load(userIds, new KeyLoader(Schema.Keys.USER_ID), multithreaded);
    }
    
    private void loadCategoryPaths()
            throws FileNotFoundException, InterruptedException, IOException {

        final KeyFileReader paths = new KeyFileReader(categoryFilename);
        this.load(paths, new CategoryPathLoader(), false);
    }
    
    private void loadReviews(final boolean compact, final boolean multithreaded)
            throws FileNotFoundException, InterruptedException {

        try (ProductReviewFileReader productReviews = new ProductReviewFileReader(productReviewFilename, compact)) {
            this.load(productReviews, new ProductReviewLoader(), multithreaded);
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    private void loadTitles(final boolean compact, final boolean multithreaded)
            throws FileNotFoundException, InterruptedException {

        try (ProductTitleFileReader productTitles = new ProductTitleFileReader(productTitleFilename, compact)) {
            this.load(productTitles, new ProductTitleLoader(), multithreaded);
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    private void loadCategoryLinks(final boolean multithreaded)
            throws FileNotFoundException, InterruptedException {

        try (CategoryLinkFileReader categories = new CategoryLinkFileReader(categoryLinkFilename)) {
            this.load(categories, new CategoryLinkLoader(), multithreaded);
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    private <T> void load(final Iterable<T> reader, final VertexLoader loader, final boolean multithreaded)
            throws InterruptedException {

        if (!multithreaded) {
            waitForAllTasks();
        }

        final Iterator<T> itty = reader.iterator();
        while (itty.hasNext()) {
            final T item = itty.next();
            if (!(item instanceof AmazonEntity) || ((AmazonEntity)item).isValid()) {
                semaphore.acquire();
                loader.init(this, item, batchSize);
                loadEntity(loader, multithreaded);
            }
        }
        
        if (!multithreaded) {
            this.commit();
        }
    }
    
    private void loadEntity(final VertexLoader loader, final boolean multithreaded) {
        if (multithreaded) {
            executor.execute(loader);
        }
        else {
            loader.run();
        }
    }
    
    public Graph getGraph() {
        return this.graph;
    }
    
    public void notifyEntityDone() {
        semaphore.release();
        if (counter.incrementAndGet()%batchSize == 0L) {
            this.commit();
        }
    }
    
    private static void waitForAllTasks() throws InterruptedException {
        semaphore.acquire(MAX_PARALLEL_TASKS);
        semaphore.release(MAX_PARALLEL_TASKS);
    }
}
