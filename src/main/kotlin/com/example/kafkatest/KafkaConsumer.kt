package com.example.kafkatest

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.CountDownLatch

@Component
class KafkaConsumer(

) {
    private val latch: CountDownLatch = CountDownLatch(1)
    lateinit var payload: String

    @KafkaListener(topics = ["\${test.topic}"])
    fun receive(consumerRecord: ConsumerRecord<Any, Any>) {
        logger.info("received payload='{}'", consumerRecord.toString())
        payload = consumerRecord.toString()
        latch.countDown()
    }

    fun getLatch(): CountDownLatch {
        return latch
    }


    companion object {
        val logger: Logger = LoggerFactory.getLogger(KafkaConsumer::class.java)
    }
}