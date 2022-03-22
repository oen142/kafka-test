package com.example.kafkatest

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import java.util.concurrent.TimeUnit


@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = ["listeners=PLAINTEXT://localhost:9092", "port=9092"]
)
internal class EmbeddedKafkaIntegrationTest {
    @Autowired
    private val consumer: KafkaConsumer? = null

    @Autowired
    private val producer: KafkaProducer? = null

    @Value("\${test.topic}")
    private val topic: String? = null

    @Test
    @Throws(Exception::class)
    fun givenEmbeddedKafkaBroker_whenSendingtoSimpleProducer_thenMessageReceived() {
        producer!!.send(topic!!, "Sending with own simple KafkaProducer")
        consumer!!.getLatch().await(10000, TimeUnit.MILLISECONDS)
        assertThat(consumer.getLatch().count).isEqualTo(0L)
        assertThat(consumer.payload).contains("embedded-test-topic")
    }
}
