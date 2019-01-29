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

const val INPUT_TOPIC = "document-prepared-events-be"
const val OUTPUT_TOPIC = "document-created-events"
const val NUMBER_RANGE_EVEN_STORE_NAME = "be-number-range-even-store"
const val NUMBER_RANGE_ODD_STORE_NAME = "be-number-range-odd-store"

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

        //val storeSupplier = Stores.persistentKeyValueStore(NUMBER_RANGE_EVEN_STORE_NAME)


        val storeSupplier = { storeName: String -> Stores.persistentKeyValueStore(storeName) }
        val storeBuilder = { storeName: String ->
            Stores.keyValueStoreBuilder(
                storeSupplier(storeName), Serdes.Integer(), numberRangeSerde
            )
        }


        listOf(
            NUMBER_RANGE_EVEN_STORE_NAME,
            NUMBER_RANGE_ODD_STORE_NAME
        ).forEach { streamBuilder.addStateStore(storeBuilder(it)) }


        val inputEventStream = streamBuilder.stream<Int, InvoicePreparedEvent>(
            INPUT_TOPIC,
            Consumed.with(Serdes.Integer(), inputEventSerde)
        )

        val outputEventStream = inputEventStream.transformValues(
            fun(): NumberRangeTransformer = NumberRangeTransformer(),
            arrayOf(NUMBER_RANGE_EVEN_STORE_NAME, NUMBER_RANGE_ODD_STORE_NAME)
        )

        outputEventStream.to(OUTPUT_TOPIC, Produced.with(Serdes.Integer(), outputEventSerde))

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


fun NumberRange(country: String, outletId: Int) =
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
        val LOG = LoggerFactory.getLogger(NumberRangeTransformer::class.java)
    }

    var context: ProcessorContext? = null
    private var numberRangeEvenStore: KeyValueStore<Int, NumberRange>? = null
    private var numberRangeOddStore: KeyValueStore<Int, NumberRange>? = null

    @Suppress("UNCHECKED_CAST")
    override fun init(context: ProcessorContext?) {
        this.context = context
        this.numberRangeEvenStore =
            this.context!!.getStateStore(NUMBER_RANGE_EVEN_STORE_NAME) as KeyValueStore<Int, NumberRange>
        this.numberRangeOddStore =
            this.context!!.getStateStore(NUMBER_RANGE_ODD_STORE_NAME) as KeyValueStore<Int, NumberRange>
    }

    override fun transform(event: InvoicePreparedEvent?): InvoiceCreatedEvent {
        LOG.info("received event: {}", event)

        val outletId = event!!.outletId
        val country = event.country
        // get current counter event
        val store = this.getStore(outletId)
        val numberRange = store.get(outletId) ?: NumberRange(country, outletId)

        val incremented = numberRange.inc()


        store.put(outletId, incremented)

        LOG.info("using number range: {}", numberRange)

        return InvoiceCreatedEvent.newBuilder()
            .setCurrency(event.currency)
            .setAmount(event.amount)
            .setOutletId(event.outletId)
            .setCountry(event.country)
            .setType(event.type)
            .setLines(event.lines)
            .setFiscalNumber("A0000000" + incremented.counter)
            .setId(event.id)
            .setAffiliate(event.affiliate)
            .build()
    }

    private fun getStore(outletId: Int): KeyValueStore<Int, NumberRange> {
        return if (outletId.rem(2) == 0) this.numberRangeEvenStore!! else this.numberRangeOddStore!!
    }

    override fun close() {
        // no-op
    }

}

