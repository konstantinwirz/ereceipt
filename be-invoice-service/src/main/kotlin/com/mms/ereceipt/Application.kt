package com.mms.ereceipt

import com.mms.ereceipt.avro.Affiliate
import com.mms.ereceipt.avro.InvoicePreparedEvent
import com.mms.ereceipt.avro.InvoiceRequestedEvent
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import java.util.*

object Application {


    val inputTopic = "ereceipt-invoice-requested-events"
    val outputTopic = "ereceipt-invoice-prepared-events"


    @JvmStatic
    fun main(args: Array<String>) {
        val config = ConfigFactory.defaultApplication()
        val bootstrapServers = config.getString("kafka.bootstrap.servers")
        val schemaRegistryUrl = config.getString("kafka.schema.registry.url")
        val applicationId = config.getString("kafka.application.id")
        val groupId = config.getString("kafka.group.id")

        val streamBuilder = StreamsBuilder()
        val invoiceRequestedSerde = SpecificAvroSerde<InvoiceRequestedEvent>().apply {
            configure(
                mapOf(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "http://legend:8081"),
                false
            )
        }

        val invoicePreparedSerde = SpecificAvroSerde<InvoicePreparedEvent>().apply {
            configure(
                mapOf("schema.registry.url" to "http://legend:8081"),
                false
            )
        }

        val inputEventStream = streamBuilder.stream<String, InvoiceRequestedEvent>(
            inputTopic,
            Consumed.with(Serdes.String(), invoiceRequestedSerde)
        )

        val mapValues = inputEventStream.mapValues { country, event ->
            println("EVENT = $event")
            InvoicePreparedEvent.newBuilder()
                .setId(event.id)
                .setType(event.type)
                .setAmount(event.amount)
                .setCurrency(event.currency)
                .setCountry(event.country)
                .setOutletId(event.outletId)
                .setAffiliate(
                    Affiliate.newBuilder().setKeyIndex(event.affiliate.keyIndex).setAlgo(event.affiliate.algo).setPayload(
                        event.affiliate.payload
                    ).build()
                )
                .setLines(event.lines)
                .build()
        }

        mapValues.to(outputTopic, Produced.with(Serdes.String(), invoicePreparedSerde))


        val props = Properties()
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
        props[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE

        val topology = streamBuilder.build(props)
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}
