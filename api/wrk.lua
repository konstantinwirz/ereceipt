wrk.method = "POST"
wrk.body = [[{
        "type": "DOWNPAYMENT",
        "country": "BE",
        "outletId": 1000,
        "amount": "1300.99",
        "currency": "EUR",
        "affiliate": {
            "name": "Mary Jones"
        },
        "lines": [
            {
                "id": "1"
            },
            {
                "id": "5"
            }
        ]
    }]]
wrk.headers["Content-Type"] = "application/json"
wrk.headers["Accept"] = "application/json"
