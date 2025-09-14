package com.example.midas.kafka;

import com.example.midas.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionListener {

    private final FirstFourCapture capture;

    @KafkaListener(topics = "${midas.transactions.topic}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Payload Transaction tx) {
        if (tx == null) {
            log.warn("Received null transaction");
            return;
        }

        String amountStr;
        try {
            if (tx.getAmount() != null) {
                amountStr = tx.getAmount().toString();
            } else {
                amountStr = "<null>";
            }
        } catch (Throwable t) {
            amountStr = "<error>";
        }

        log.info("Received transaction id={} amount={}", tx.getId(), amountStr);
        capture.capture(amountStr);
    }
}
