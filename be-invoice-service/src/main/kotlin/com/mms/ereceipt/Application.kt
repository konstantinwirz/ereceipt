package com.mms.ereceipt

import com.mms.ereceipt.avro.Affiliate
import com.mms.ereceipt.avro.InvoicePreparedEvent
import com.mms.ereceipt.avro.InvoiceRequestedEvent
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import java.util.*

object Application {


    val inputTopic = "ereceipt-invoice-requested-events"
    val outputTopic = "ereceipt-invoice-prepared-events"


    @JvmStatic
    fun main(args: Array<String>) {
        val streamBuilder = StreamsBuilder()
        val invoiceRequestedSerde = SpecificAvroSerde<InvoiceRequestedEvent>().apply {
            configure(
                mapOf("schema.registry.url" to "http://legend:8081"),
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
        props["bootstrap.servers"] = "legend:29092"
        props["application.id"] = "be-invoice-service"
        val topology = streamBuilder.build(props)
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}
