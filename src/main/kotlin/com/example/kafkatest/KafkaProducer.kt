package com.example.kafkatest

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class KafkaProducer(
    @Autowired
    private val kafkaTemplate: KafkaTemplate<String , String>
) {

    fun send(topic: String , payload : String){
        logger.info("sending payload = '{}' to topic='{}'" , payload , topic)
        kafkaTemplate.send(topic , payload)
    }

    companion object{
        val logger : Logger = LoggerFactory.getLogger(KafkaProducer::class.java)
    }


}