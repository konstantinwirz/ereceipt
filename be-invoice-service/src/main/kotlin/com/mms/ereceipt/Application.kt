package com.mms.ereceipt

import com.mms.ereceipt.avro.Affiliate
import com.mms.ereceipt.avro.InvoicePreparedEvent
import com.mms.ereceipt.avro.InvoiceRequestedEvent
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import java.util.*

object Application {


    val inputTopic = "document-requested-events"

    val outputTopics = mapOf(
        "ES" to "document-prepared-events-es",
        "BE" to "document-prepared-events-be"
    )

    val outputTopicUnknown = "document-prepared-events-unknown"

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
                mapOf(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl),
                false
            )
        }

        val invoicePreparedSerde = SpecificAvroSerde<InvoicePreparedEvent>().apply {
            configure(
                mapOf(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl),
                false
            )
        }

        val inputEventStream = streamBuilder.stream<Int, InvoiceRequestedEvent>(
            inputTopic,
            Consumed.with(Serdes.Integer(), invoiceRequestedSerde)
        )

        val mapValues = inputEventStream.mapValues { outletId, event ->
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

        mapValues.to(
            { key, event, recordContext ->
                outputTopics.getOrDefault(event.country, outputTopicUnknown)
            },
            Produced.with(Serdes.Integer(), invoicePreparedSerde)
        )


        val props = Properties()
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
        props[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE
        props[ProducerConfig.PARTITIONER_CLASS_CONFIG] = OutletPartitioner::class.java.canonicalName

        val topology = streamBuilder.build()
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}


class OutletPartitioner : Partitioner {
    override fun configure(configs: MutableMap<String, *>?) {
        // no-op
    }

    override fun close() {
        // no-op
    }

    override fun partition(
        topic: String,
        key: Any,
        keyBytes: ByteArray,
        value: Any,
        valueBytes: ByteArray,
        cluster: Cluster
    ): Int {
        val outletId = (keyBytes[0].toInt() shl 24) and 0xff000000.toInt() or
                        (keyBytes[1].toInt() shl 16) and 0x00ff0000 or
                        (keyBytes[2].toInt() shl 8) and 0x0000ff00 or
                        (keyBytes[3].toInt() shl 0) and 0x000000ff
        return if (outletId.rem(2) == 0) 0 else 1

    }

}