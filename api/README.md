### post a invoice

```bash
> http POST :8080/invoices type=DOWNPAYMENT country=BE outletId=1234 amount=1300.99 currency=EUR affiliate:='{"name":"John Doe"}' lines:='[{"id":"1"}, {"id":"5"}]'
```

### perform a load test using [wrk](https://github.com/wg/wrk)

```bash
> wrk -t16 -c400 -d10s -s wrk.lua http://localhost:8080/invoices

Running 10s test @ http://localhost:8080/invoices
  16 threads and 400 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   161.44ms   48.55ms 379.08ms   71.41%
    Req/Sec   153.56     55.27   650.00     75.11%
  24595 requests in 10.09s, 3.07MB read
Requests/sec:   2436.53
Transfer/sec:    311.70KB
```
