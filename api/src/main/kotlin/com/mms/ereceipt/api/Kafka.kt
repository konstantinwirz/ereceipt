package com.mms.ereceipt.api

import com.mms.ereceipt.avro.InvoiceRequestedEvent
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.ktor.config.HoconApplicationConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*


fun createProducer(): Producer<String, InvoiceRequestedEvent> {
    val config = HoconApplicationConfig(ConfigFactory.load()).config("kafka")
    val properties = Properties()
    properties["bootstrap.servers"] = config.property("bootstrap.servers").getString()
    properties["key.serializer"] = StringSerializer::class.java.canonicalName
    properties["value.serializer"] = KafkaAvroSerializer::class.java.canonicalName
    properties["schema.registry.url"] = config.property("schema.registry.url").getString()
    return KafkaProducer(properties)
}