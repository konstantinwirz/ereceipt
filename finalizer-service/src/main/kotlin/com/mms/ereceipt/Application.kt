package com.mms.ereceipt

import com.mms.ereceipt.avro.InvoiceCreatedEvent
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.slf4j.LoggerFactory
import java.util.*


const val INPUT_TOPIC = "ereceipt-invoice-created-events"


object Application {
    val LOG = LoggerFactory.getLogger(Application.javaClass)

    @JvmStatic
    fun main(args: Array<String>) {
        val config = ConfigFactory.defaultApplication()
        val bootstrapServers = config.getString("kafka.bootstrap.servers")
        val schemaRegistryUrl = config.getString("kafka.schema.registry.url")
        val applicationId = config.getString("kafka.application.id")
        val groupId = config.getString("kafka.group.id")

        val inputEventSerde = SpecificAvroSerde<InvoiceCreatedEvent>().apply {
            configure(
                mapOf(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl),
                false
            )
        }

        val streamBuilder = StreamsBuilder()

        streamBuilder
            .stream<String, InvoiceCreatedEvent>(INPUT_TOPIC, Consumed.with(Serdes.String(), inputEventSerde))
            .foreach { country, event ->
                LOG.info("received: {}", event)
            }


        val props = Properties()
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
        props[ConsumerConfig.GROUP_ID_CONFIG] = groupId

        val topology = streamBuilder.build(props)
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}
