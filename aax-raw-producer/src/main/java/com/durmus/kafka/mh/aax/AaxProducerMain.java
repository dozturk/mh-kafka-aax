package com.durmus.kafka.mh.aax;

import com.durmus.kafka.mh.avro.aax.AAX;
import com.durmus.kafka.mh.aax.runnable.AaxAvroProducerThread;
import com.durmus.kafka.mh.aax.runnable.AaxFetcherThread;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

public class AaxProducerMain {


    private Logger log = LoggerFactory.getLogger(AaxProducerMain.class.getSimpleName());

    // thread safe queue which blocks when full.
    private ExecutorService executor;
    private CountDownLatch latch;
    private AaxFetcherThread csvParser;
    private AaxAvroProducerThread aaxProducer;

    public static void main(String[] args) {
        AaxProducerMain app = new AaxProducerMain();
        app.start();
    }

    private AaxProducerMain() {
        AppConfig appConfig = new AppConfig(ConfigFactory.load());
        latch = new CountDownLatch(2);
        executor = Executors.newFixedThreadPool(2);
        ArrayBlockingQueue<AAX> aaxQueue = new ArrayBlockingQueue<>(appConfig.getQueueCapacity());
        csvParser = new AaxFetcherThread(appConfig, aaxQueue, latch);
        aaxProducer = new AaxAvroProducerThread(appConfig, aaxQueue, latch);
    }

    private void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!executor.isShutdown()){
                log.info("Shutdown requested");
                shutdown();
            }
        }));

        log.info("Application started!");
        executor.submit(csvParser);
        executor.submit(aaxProducer);
        log.info("Stuff submit");
        try {
            log.info("Latch await");
            latch.await();
            log.info("Threads completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            shutdown();
            log.info("Application closed succesfully");
        }

    }


    private void shutdown() {
        if (!executor.isShutdown()) {
            log.info("Shutting down");
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(2000, TimeUnit.MILLISECONDS)) { //optional *
                    log.warn("Executor did not terminate in the specified time."); //optional *
                    List<Runnable> droppedTasks = executor.shutdownNow(); //optional **
                    log.warn("Executor was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed."); //optional **
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}

