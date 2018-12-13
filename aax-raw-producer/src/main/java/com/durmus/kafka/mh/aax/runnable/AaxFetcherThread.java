package com.durmus.kafka.mh.aax.runnable;

import com.durmus.kafka.mh.aax.AppConfig;
import com.durmus.kafka.mh.aax.client.AaxCsvParser;
import com.durmus.kafka.mh.avro.aax.AAX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class AaxFetcherThread  implements Runnable {

    private Logger log = LoggerFactory.getLogger(AaxFetcherThread.class.getSimpleName());

    private final AppConfig appConfig;
    private final ArrayBlockingQueue<AAX> aaxQueue;
    private final CountDownLatch latch;
    private final AaxCsvParser aaxCsvParser;
    private final List<String> headers = Arrays.asList("id", "mhkundennummer", "guid", "crmtkundennummer", "carmenkundennummer", "kek", "name",
            "adresse", "mail", "zahlart", "vertragsnummer", "produktid",  "produktname", "mvlz",
            "vertragsstart", "vertragsende", "kundigungsdatum", "kundigungsmodus", "kundigungsgrund", "mvlzendedatum", "kundigungsdatumbindefrist");




    public AaxFetcherThread(AppConfig appConfig, ArrayBlockingQueue<AAX> aaxQueue, CountDownLatch latch) {
        this.appConfig = appConfig;
        this.aaxQueue = aaxQueue;
        this.latch = latch;
        aaxCsvParser = new AaxCsvParser(headers, appConfig.getRegex(), appConfig.getPath());
    }

    @Override
    public void run() {
        try {
            Boolean keepOnRunning = true;
            while (keepOnRunning){
                List<AAX> data;

                try {
                    data = aaxCsvParser.getNextFile();
                    log.info("Fetched " + data.size() + " data");
                    if (data.size() == 0){
                        keepOnRunning = false;
                    } else {
                        // this may block if the queue is full - this is flow control
                        log.info("Queue size :" + aaxQueue.size());
                        for (AAX datum : data){
                            aaxQueue.put(datum);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(500);
                } finally {
                    Thread.sleep(50);
                }
            }
        } catch (Exception e) {
            log.warn("Csv Parser interrupted");
        } finally {
            this.close();
        }
    }

    private void close() {
        log.info("Closing");
        aaxCsvParser.close();
        latch.countDown();
        log.info("Closed");
    }
}
