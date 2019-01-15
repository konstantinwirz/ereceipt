### post a invoice

```bash
> http POST :8080/invoices type=DOWNPAYMENT country=BE outletId=1234 amount=1300.99 currency=EUR affiliate:='{"name":"John Doe"}' lines:='[{"id":"1"}, {"id":"5"}]'
```