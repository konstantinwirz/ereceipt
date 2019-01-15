package com.mms.ereceipt.api

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.mms.ereceipt.avro.InvoiceRequestedEvent
import org.apache.avro.Conversions
import java.math.BigDecimal
import java.util.*

data class InvoiceLine(
    val id: String,
    val name: String = "",
    val price: BigDecimal = BigDecimal.ZERO
)

data class Affiliate(
    val name: String
)


data class InvoiceRequest(
    val id: String = UUID.randomUUID().toString(),
    val type: String,
    val amount: BigDecimal,
    val currency: String,
    val country: String,
    val outletId: String,
    val affiliate: Affiliate,
    val lines: List<InvoiceLine> = emptyList()
)


fun InvoiceLine.toAvro(): com.mms.ereceipt.avro.InvoiceLine =
    com.mms.ereceipt.avro.InvoiceLine.newBuilder()
        .setId(this.id)
        .setName(this.name)
        .setPrice(this.price)
        .build()

fun Affiliate.toAvro(): com.mms.ereceipt.avro.Affiliate =
        com.mms.ereceipt.avro.Affiliate.newBuilder()
            .setKeyIndex(2007)
            .setAlgo("EC")
            .setPayload(Encryptor.encryptAffiliate(this))
            .build()

fun InvoiceRequest.toAvro(): InvoiceRequestedEvent =
        InvoiceRequestedEvent.newBuilder()
            .setId(this.id)
            .setType(this.type)
            .setAmount(this.amount)
            .setOutletId(this.outletId)
            .setCurrency(this.currency)
            .setCountry(this.country)
            .setLines(this.lines.map(InvoiceLine::toAvro))
            .setAffiliate(this.affiliate.toAvro())
            .build()
