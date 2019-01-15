package com.mms.ereceipt

import com.mms.ereceipt.avro.FiscalNumberGeneratedEvent
import com.mms.ereceipt.avro.InvoicePreparedEvent
import com.mms.ereceipt.avro.NumberRange
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
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
import java.time.LocalDate
import java.util.*


object Application {

    val inputTopic = "ereceipt-invoice-prepared-events"
    val outputTopic = "ereceipt-fiscal-number-generated-events"

    @JvmStatic
    fun main(args: Array<String>) {

        val config = ConfigFactory.defaultApplication()
        val booststrapServers = config.getString("kafka.bootstrap.servers")
        val schemaRegistryUrl = config.getString("kafka.schema.registry.url")
        val applicationId = config.getString("kafka.application.id")
        val groupId = config.getString("kafka.group.id")

        val inputEventSerde = SpecificAvroSerde<InvoicePreparedEvent>().apply {
            configure(
                mapOf("schema.registry.url" to schemaRegistryUrl),
                false
            )
        }

        val outputEventSerde = SpecificAvroSerde<FiscalNumberGeneratedEvent>().apply {
            configure(
                mapOf("schema.registry.url" to schemaRegistryUrl),
                false
            )
        }

        val numberRangeSerde = SpecificAvroSerde<NumberRange>().apply {
            configure(
                mapOf("schema.registry.url" to schemaRegistryUrl),
                false
            )
        }

        val streamBuilder = StreamsBuilder()

        val storeSupplier = Stores.persistentKeyValueStore("NumberRangeStore")
        val storeBuilder = Stores.keyValueStoreBuilder(
            storeSupplier, Serdes.String(), numberRangeSerde)

        streamBuilder.addStateStore(storeBuilder)

        val inputEventStream = streamBuilder.stream<String, InvoicePreparedEvent>(
            inputTopic,
            Consumed.with(Serdes.String(), inputEventSerde)
        )

        val outputEventStream = inputEventStream.transformValues(
            fun(): NumberRangeTransformer = NumberRangeTransformer(),
            arrayOf("NumberRangeStore")
        ).mapValues { numberRange ->
            FiscalNumberGeneratedEvent.newBuilder()
                .setCounter(numberRange.counter)
                .setCountry(numberRange.country)
                .setVirtualDevice("001")
                .setOutletId(numberRange.outletId)
                .setFiscalNumber("A001000000000" + numberRange.counter)
                .build()
        };

        outputEventStream.to(outputTopic, Produced.with(Serdes.String(), outputEventSerde))

        val props = Properties()
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = booststrapServers
        props["kafka.schema.registry.url"] = schemaRegistryUrl
        props[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
        props["group.id"] = groupId

        val topology = streamBuilder.build(props)
        val streams = KafkaStreams(topology, props)
        streams.start()
    }

}

/*
data class NumberRange(
    val country: String,
    val outletId: String,
    val counter: Long = 0,
    val year: Int = LocalDate.now().year
) {
    operator fun inc() = this.copy(counter = this.counter.inc())
    fun reset() = this.copy(counter = 0)
}
*/

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


class NumberRangeTransformer : ValueTransformer<InvoicePreparedEvent, NumberRange> {
    var context: ProcessorContext? = null
    val storeName = "NumberRangeStore"
    var stateStore: KeyValueStore<String, NumberRange>? = null

    @Suppress("UNCHECKED_CAST")
    override fun init(context: ProcessorContext?) {
        this.context = context
        this.stateStore = this.context!!.getStateStore(this.storeName) as KeyValueStore<String, NumberRange>?
    }

    override fun transform(value: InvoicePreparedEvent?): NumberRange {
        val outletId = value!!.outletId
        val country = value.country
        // get current counter value
        val numberRange = this.stateStore!!.get(outletId) ?: NumberRange(country, outletId)

        val incremented = numberRange.inc();
        stateStore!!.put(outletId, incremented)

        return incremented
    }

    override fun close() {
        // no-op
    }

}