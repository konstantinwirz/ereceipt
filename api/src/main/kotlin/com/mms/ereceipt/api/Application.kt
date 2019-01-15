package com.mms.ereceipt.api

import com.fasterxml.jackson.module.kotlin.MissingKotlinParameterException
import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.application.log
import io.ktor.features.CallLogging
import io.ktor.features.ContentNegotiation
import io.ktor.http.HttpStatusCode
import io.ktor.jackson.jackson
import io.ktor.request.path
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.post
import io.ktor.routing.routing
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.event.Level

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

@kotlin.jvm.JvmOverloads
fun Application.module(@Suppress("UNUSED_PARAMETER") testing: Boolean = false) {


    install(CallLogging) {
        level = Level.INFO
        filter { call -> call.request.path().startsWith("/") }
    }

    install(ContentNegotiation) {
        jackson {
        }
    }

    routing {
        post("/invoices") {
            try {
                val invoiceRequest = call.receive<InvoiceRequest>()
                val enrichedRequest = Enricher.enrich(invoiceRequest)
                val errors = Validator.validate(enrichedRequest)
                if (errors.isEmpty()) {
                    val event = enrichedRequest.toAvro()
                    log.info("event = {}", event)
                    val producer = createProducer()
                    val record = ProducerRecord("ereceipt-invoice-requested-events", event.country, event)
                    producer.send(record) { metadata, e ->
                        if (e != null) log.error("failed to send message: {}", e.message)
                        else log.info("sent message: {}", metadata)
                    }

                    call.respond(mapOf("id" to enrichedRequest.id))
                } else {
                    call.respond(HttpStatusCode.BadRequest, errors)
                }
            } catch (e: MissingKotlinParameterException) {
                call.respond(
                    HttpStatusCode.BadRequest, mapOf(
                        "error" to "failed to parse json payload",
                        "details" to e.message
                    )
                )
            } catch (e: Exception) {
                e.printStackTrace()
                call.respond(
                    HttpStatusCode.InternalServerError, mapOf(
                        "error" to "internal error",
                        "details" to e.message
                    )
                )
            }
        }
    }
}

