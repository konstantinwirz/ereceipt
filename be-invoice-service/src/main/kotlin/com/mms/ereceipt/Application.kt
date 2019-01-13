package com.mms.ereceipt

import com.mms.ereceipt.avro.InvoiceRequestedEvent
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import java.util.*

object Application {

    val inputTopic = "ereceipt-invoice-requested-events"
    val outputTopic = "ereceipt-invoice-prepared-events"


    @JvmStatic
    fun main(args: Array<String>) {
        val streamBuilder = StreamsBuilder()
        val avroSerde = GenericAvroSerde().apply {
            configure(
                mapOf("schema.registry.url" to "http://legend:8081")
                , false
            )
        }

        val inputEventStream = streamBuilder.stream<String, InvoiceRequestedEvent>(
            inputTopic,
            Consumed.with(Serdes.String(), InvoiceRequestedEventSerde())
        )

        inputEventStream.foreach { country, event ->
            println("COUNTRY = $country")
            println("EVENT = $event")
        }

        val props = Properties()
        props["bootstrap.servers"] = "legend:29092"
        props["application.id"] = "be-invoice-service"
        val topology = streamBuilder.build(props)
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}


class InvoiceRequestedEventSerializer : Serializer<InvoiceRequestedEvent> {
    val avroSerializer = KafkaAvroSerializer()

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) =
        avroSerializer.configure(configs, isKey)

    override fun serialize(topic: String?, data: InvoiceRequestedEvent?): ByteArray =
        avroSerializer.serialize(topic, data)

    override fun close() =
        avroSerializer.close()
}

class InvoiceRequestedEventDeserializer : Deserializer<InvoiceRequestedEvent> {
    val avroDeserializer = KafkaAvroDeserializer()

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) =
        avroDeserializer.configure(configs, isKey)

    override fun deserialize(topic: String?, data: ByteArray?): InvoiceRequestedEvent =
        avroDeserializer.deserialize(topic, data) as InvoiceRequestedEvent

    override fun close() =
        avroDeserializer.close()
}

class InvoiceRequestedEventSerde : Serde<InvoiceRequestedEvent> {

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}

    override fun deserializer(): Deserializer<InvoiceRequestedEvent> =
        InvoiceRequestedEventDeserializer()

    override fun close() {}

    override fun serializer(): Serializer<InvoiceRequestedEvent> =
        InvoiceRequestedEventSerializer()
}