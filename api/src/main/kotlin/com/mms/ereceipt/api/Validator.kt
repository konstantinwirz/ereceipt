package com.mms.ereceipt.api

import java.math.BigDecimal

data class ValidationError(
    val error: String = "validation error",
    val details: String
)

object Validator {

    fun validate(request: InvoiceRequest): List<ValidationError> {
        val errors = arrayListOf<ValidationError>()
        with(request) {
            if (type != "DOWNPAYMENT") errors += ValidationError(details = "wrong invoice type")
            if (currency != "EUR") errors += ValidationError(details = "wrong currency")
            if (lines.isEmpty()) errors += ValidationError(details = "no lines given")
            if (!checkAmount(request))  errors += ValidationError(details = "wrong amount")
        }
        return errors
    }

    private fun checkAmount(request: InvoiceRequest) =
        request.amount ==
                request.lines.map { it.price }.fold(BigDecimal.ZERO) { a, b -> a + b }
}