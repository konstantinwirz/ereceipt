package com.mms.ereceipt.api

import com.mms.ereceipt.avro.InvoiceRequestedEvent
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.ktor.config.HoconApplicationConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*


fun createProducer(): Producer<String, InvoiceRequestedEvent> {
    val config = HoconApplicationConfig(ConfigFactory.load()).config("kafka")
    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = config.property("bootstrap.servers").getString()
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.canonicalName
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java.canonicalName
    properties[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = config.property("schema.registry.url").getString()
    properties[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = "true"
    properties[ProducerConfig.RETRIES_CONFIG] = "1"
    properties[ProducerConfig.ACKS_CONFIG] = "all"

    return KafkaProducer<String, InvoiceRequestedEvent>(properties)
}