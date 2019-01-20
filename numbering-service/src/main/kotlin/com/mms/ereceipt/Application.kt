package com.mms.ereceipt

import com.mms.ereceipt.avro.InvoiceCreatedEvent
import com.mms.ereceipt.avro.InvoicePreparedEvent
import com.mms.ereceipt.avro.NumberRange
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.util.*

const val INPUT_TOPIC = "ereceipt-invoice-prepared-events"
const val OUTPUT_TOPIC = "ereceipt-invoice-created-events"
const val NUMBER_RANGE_STORE_NAME = "number-range-store"


object Application {

    @JvmStatic
    fun main(args: Array<String>) {

        val config = ConfigFactory.defaultApplication()
        val bootstrapServers = config.getString("kafka.bootstrap.servers")
        val schemaRegistryUrl = config.getString("kafka.schema.registry.url")
        val applicationId = config.getString("kafka.application.id")
        val groupId = config.getString("kafka.group.id")


        val inputEventSerde = SpecificAvroSerde<InvoicePreparedEvent>().apply {
            configure(
                mapOf(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl),
                false
            )
        }

        val outputEventSerde = SpecificAvroSerde<InvoiceCreatedEvent>().apply {
            configure(
                mapOf(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl),
                false
            )
        }

        val numberRangeSerde = SpecificAvroSerde<NumberRange>().apply {
            configure(
                mapOf(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl),
                false
            )
        }

        val streamBuilder = StreamsBuilder()

        val storeSupplier = Stores.persistentKeyValueStore(NUMBER_RANGE_STORE_NAME)
        val storeBuilder = Stores.keyValueStoreBuilder(
            storeSupplier, Serdes.String(), numberRangeSerde
        )

        streamBuilder.addStateStore(storeBuilder)

        val inputEventStream = streamBuilder.stream<String, InvoicePreparedEvent>(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), inputEventSerde)
        )

        val outputEventStream = inputEventStream.transformValues(
            fun(): NumberRangeTransformer = NumberRangeTransformer(),
            arrayOf(NUMBER_RANGE_STORE_NAME)
        )

        outputEventStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), outputEventSerde))

        val props = Properties()
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
        props[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE

        val topology = streamBuilder.build()
        val streams = KafkaStreams(topology, props)
        streams.start()
    }

}


fun NumberRange(country: String, outletId: String) =
    NumberRange.newBuilder()
        .setCountry(country)
        .setOutletId(outletId)
        .setYear(LocalDate.now().year)
        .setCounter(0)
        .build()

fun NumberRange.inc() =
    NumberRange.newBuilder()
        .setCountry(this.country)
        .setOutletId(this.outletId)
        .setYear(this.year)
        .setCounter(this.counter + 1)
        .build()

class NumberRangeTransformer : ValueTransformer<InvoicePreparedEvent, InvoiceCreatedEvent> {

    companion object {
        val LOG = LoggerFactory.getLogger(NumberRangeTransformer.javaClass)
    }

    var context: ProcessorContext? = null
    private var numberRangeStore: KeyValueStore<String, NumberRange>? = null

    @Suppress("UNCHECKED_CAST")
    override fun init(context: ProcessorContext?) {
        this.context = context
        this.numberRangeStore =
                this.context!!.getStateStore(NUMBER_RANGE_STORE_NAME) as KeyValueStore<String, NumberRange>
    }

    override fun transform(value: InvoicePreparedEvent?): InvoiceCreatedEvent {
        LOG.info("received event: {}", value)

        val outletId = value!!.outletId
        val country = value.country
        // get current counter value
        val numberRange = this.numberRangeStore!!.get(outletId) ?: NumberRange(country, outletId)

        val incremented = numberRange.inc()

        numberRangeStore!!.put(outletId, incremented)

        LOG.info("using number range: {}", numberRange)

        return InvoiceCreatedEvent.newBuilder()
            .setCurrency(value.currency)
            .setAmount(value.amount)
            .setOutletId(value.outletId)
            .setCountry(value.country)
            .setType(value.type)
            .setLines(value.lines)
            .setFiscalNumber("A0000000" + incremented.counter)
            .setId(value.id)
            .setAffiliate(value.affiliate)
            .build()
    }

    override fun close() {
        // no-op
    }

}

