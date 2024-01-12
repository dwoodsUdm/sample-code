# sample-code

### Samples

Author: @dwoodsUdm

[Regression Test Lambda](./reg-check/main.go)

Is triggered by an S3 bucket write, fetches the data from S3, generates an API call to the dev cluster to run the same request, and compares the results.

[Customer Data Migration](./data-migration/main.go)

A non-production script I wrote to migrate a customer's data. It's ugly, but has examples of coroutines, channels, structs with sort methods, marshalling in and out of structs, S3 integration, REST API calls, etc.

[Google BigQuery Data Summarization](./summarize-data/main.go)

Unpacks detail ad-tracking records, stored from a complex JSON including nested arrays and structures, and summarizes into flat reporting tables. Shows some SQL skills (particulary analytic functions).

### Cloning
```bash
git clone git@github.com:dwoodsUdm/sample-code.git
```

