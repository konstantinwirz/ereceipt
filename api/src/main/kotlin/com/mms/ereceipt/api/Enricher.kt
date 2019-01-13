package com.mms.ereceipt.api

import java.math.BigDecimal


object Enricher {

    fun enrich(request: InvoiceRequest): InvoiceRequest {
        val lines = request.lines.map { line ->
            val mayBeProduct = products.find { it.id == line.id }
            if (mayBeProduct == null) {
                line // returns the unchanged line
            } else {
                line.copy(name = mayBeProduct.name, price = mayBeProduct.price)
            }
        }

        return request.copy(lines = lines)
    }


}


data class Product(
    val id: String,
    val name: String,
    val price: BigDecimal
)

val products = setOf(
    Product("1", "Samsung TV", BigDecimal("900.99")),
    Product("2", "iPhone X", BigDecimal("849.99")),
    Product("3", "Dell Monitor 24", BigDecimal("300.00")),
    Product("4", "USB Stick 64GB", BigDecimal("18.99")),
    Product("5", "PS4 PRO", BigDecimal("400.00"))
)