package com.mms.ereceipt

import com.mms.ereceipt.avro.FiscalNumberGeneratedEvent
import com.mms.ereceipt.avro.InvoiceCreatedEvent
import com.mms.ereceipt.avro.InvoicePreparedEvent
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.time.Duration
import java.util.*


object Application {

    val inputEventsTopic = "ereceipt-invoice-prepared-events"
    val inputNumbersTopic = "ereceipt-fiscal-number-generated-events"
    val outputTopic = "ereceipt-invoice-created-events"

    @JvmStatic
    fun main(args: Array<String>) {
        val config = ConfigFactory.defaultApplication()
        val bootstrapServers = config.getString("kafka.bootstrap.servers")
        val schemaRegistryUrl = config.getString("kafka.schema.registry.url")
        val applicationId = config.getString("kafka.application.id")
        val groupId = config.getString("kafka.group.id")

        val inputEventSerde = SpecificAvroSerde<InvoicePreparedEvent>().apply {
            configure(
                mapOf("schema.registry.url" to schemaRegistryUrl),
                false
            )
        }

        val inputNumberEventSerde = SpecificAvroSerde<FiscalNumberGeneratedEvent>().apply {
            configure(
                mapOf("schema.registry.url" to schemaRegistryUrl),
                false
            )
        }

        val outputEventSerde = SpecificAvroSerde<InvoiceCreatedEvent>().apply {
            configure(
                mapOf("schema.registry.url" to schemaRegistryUrl),
                false
            )
        }

        val streamBuilder = StreamsBuilder()

        val inputEventStream = streamBuilder.stream(inputEventsTopic, Consumed.with(Serdes.String(), inputEventSerde))
        val inputNumberStream =
            streamBuilder.stream(inputNumbersTopic, Consumed.with(Serdes.String(), inputNumberEventSerde))
        val joinedStream = inputEventStream.join(
            inputNumberStream,
            InvoiceNumberJoiner(),
            JoinWindows.of(Duration.ofMinutes(1)),
            Joined.with(Serdes.String(), inputEventSerde, inputNumberEventSerde)
        )

        joinedStream.to(outputTopic, Produced.with(Serdes.String(), outputEventSerde))

        val props = Properties()
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props["schema.registry.url"] = schemaRegistryUrl
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
        props[ConsumerConfig.GROUP_ID_CONFIG] = groupId

        val topology = streamBuilder.build(props)
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}

class InvoiceNumberJoiner : ValueJoiner<InvoicePreparedEvent, FiscalNumberGeneratedEvent, InvoiceCreatedEvent> {

    override fun apply(
        invoicePreparedEvent: InvoicePreparedEvent?,
        fiscalNumberGeneratedEvent: FiscalNumberGeneratedEvent?
    ): InvoiceCreatedEvent =
        InvoiceCreatedEvent.newBuilder()
            .setAffiliate(invoicePreparedEvent!!.affiliate)
            .setId(invoicePreparedEvent.id)
            .setFiscalNumber(fiscalNumberGeneratedEvent!!.fiscalNumber)
            .setLines(invoicePreparedEvent.lines)
            .setType(invoicePreparedEvent.type)
            .setCountry(invoicePreparedEvent.country)
            .setOutletId(invoicePreparedEvent.outletId)
            .setAmount(invoicePreparedEvent.amount)
            .setCurrency(invoicePreparedEvent.currency)
            .build()

}