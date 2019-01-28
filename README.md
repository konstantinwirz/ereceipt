# fiscal services

## proof of reliable fiscal number generation

### test environment setup

1. api service recevies requests for different countries (es, be)
2. converts those requests to avro events and puts them into 'document-requested-events' topic
3. document-service consumes the above events and puts the outcome to country separated topics:
    be-document-prepared-events, es-document-prepared-events, using custom key partitioner, odd outlet number in first partitition, even outlet numbers in the second one
4. there are two numbering sevices: be-numbering-service, es-numbering service. Each service manages 2 persistent stores: for odd and even outlet numbers. After the numbers have been generated, they will be published in document-created-events queue.
5. We need to ensure that the generated numbers are gapless and not used multiple times fro different documents. For that we will write the numbers in text files, seprated by outlet numbers.


