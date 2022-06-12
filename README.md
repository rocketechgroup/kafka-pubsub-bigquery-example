# Apache Kafka to PubSub and BigQuery example

The example uses www.cloudkarafka.com to setup the free Kafka Cluster and relay messages to PubSub first then BigQuery

## Getting started

Setup your free Apache Kafka instance here: https://www.cloudkarafka.com

### Configuration (for both producer and consumer)

* `export CLOUDKARAFKA_BROKERS="host1:9094,host2:9094,host3:9094"`
  Hostnames can be found in the Details view in for your CloudKarafka instance.
* `export CLOUDKARAFKA_USERNAME="username"`
  Username can be found in the Details view in for your CloudKarafka instance.
* `export CLOUDKARAFKA_PASSWORD="password"`
  Password can be found in the Details view in for your CloudKarafka instance.
* `export CLOUDKARAFKA_TOPIC="username-topic"`
  Topic should be the same as your username followed by a dash before the topic.

### Install dependencies

```
pip install -r requirements.txt
```

### Run Producer

```
python producer.py
```

### Run Consumer

Configuration (for consumer only - required by pubsub)

* `export PROJECT_ID=gcpprojectid"`
  Your GCP project id
* `export TOPIC_ID=kafkarelaytpic"`
  The id of the Pubsub topic, i.e. kafka-relay

```
python consumer.py
```

## Adding a Root CA

In some cases the CloudKarafka Root CA may need to be manually added to the example, particularly if you are seeing the
error:

```
Failed to verify broker certificate: unable to get local issuer certificate 
```

returned when you run the example. If this is the case you will need to download
the [CloudKarakfa Root CA from our FAQ page](https://www.cloudkarafka.com/docs/faq.html) and place it in the
python-kafka-example directory, then add the following line into the `conf {...}` section:

```
'ssl.ca.location': 'cloudkarafka.ca'
```

This should resolve the error and allow for successful connection to the server.
