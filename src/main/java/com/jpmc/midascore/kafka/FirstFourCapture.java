package com.jpmc.midascore.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class FirstFourCapture {
    private final List<String> amounts = new ArrayList<>();
    private final CountDownLatch latch = new CountDownLatch(4);
    private final Object lock = new Object();

    public void capture(String amount) {
        synchronized (lock) {
            if (amounts.size() < 4) {
                amounts.add(amount);
                latch.countDown();
                log.info("Captured #{} -> {}", amounts.size(), amount);
                if (amounts.size() == 4) {
                    try {
                        Files.write(Paths.get("first-four-amounts.txt"),
                                String.join(",", amounts).getBytes());
                        log.info("Wrote first-four-amounts.txt: {}", amounts);
                    } catch (IOException e) {
                        log.error("Failed to write first-four-amounts.txt", e);
                    }
                }
            }
        }
    }

    /**
     * Optional — if you want to programmatically wait for values in code.
     */
    public List<String> awaitAndGet(long timeout, TimeUnit unit) throws InterruptedException {
        latch.await(timeout, unit);
        synchronized (lock) {
            return new ArrayList<>(amounts);
        }
    }
}
