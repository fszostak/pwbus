# pwbus

An integration bus capable of performing message transformation and managing task execution. Message exchange between plataforms, like HTTP, Redis, RabbitMQ, Apache Kafta, AWS SQS, MongoDB.

## Features

- Message transformation
- Task execution management
- Asynchronous communication
- Connectors


Install:

```
$ pip3 install pwbus
```

Start the server:

```
$ pwbus -f pwbus-registry.json start
```

Note:
Set "engine.production = false" to force reload task module always.

Sample: pwbus-registry.json

```
[
	{
		"_comment1": "----------------------------------------------------------",
		"_comment2": "### Channel for PWBus Demo Pseudosync REDIS",
		"_comment3": "----------------------------------------------------------",

		"channel": "app-channel-pseudosync-redis",

		"engine.enabled": true,
		"engine.debug": true,
		"engine.production": false,

		"flow.in.resource_type": "http",
		"flow.in.resource_name": "/app/v1/request",
		"flow.in.payload_format": "json",

		"flow.out.resource_type": "redis",
		"flow.out.host": "redis",
		"flow.out.port": 6379,
		"flow.out.resource_name": "app-channel-pseudosync.request.app-in",
		"flow.out.payload_format": "json",
		"flow.out.ttl_seconds": 0,

		"flow.out.reply_to": "app-channel-pseudosync.response.app-out",

		"flow.message_dump": false,
		"flow.execute_services": false
	},
	{
		"channel": "app-channel-pseudosync-fake-redis",
		"_comment": "### Channel for PWBus Demo Pseudosync REDIS",

		"engine.enabled": true,
		"engine.start_threads": 10,
		"engine.debug": true,

		"flow.in.resource_type": "redis",
		"flow.in.host": "redis",
		"flow.in.port": 6379,
		"flow.in.resource_name": "app-channel-pseudosync.request.app-in",
		"flow.in.payload_format": "json",

		"flow.out.resource_type": "redis",
		"flow.out.host": "redis",
		"flow.out.port": 6379,
		"flow.out.resource_name": "app-channel-pseudosync.response.app-out",
		"flow.out.payload_format": "json",
		"flow.out.ttl_seconds": 20,

		"flow.message_dump": false,
		"flow.execute_services": true
	},

	{
		"_comment1": "----------------------------------------------------------",
		"_comment2": "### Channel for PWBus Demo Pseudosync RabbitMQ",
		"_comment3": "----------------------------------------------------------",

		"channel": "app-channel-pseudosync-rabbitmq",

		"engine.enabled": true,
		"engine.debug": true,

		"flow.in.resource_type": "http",
		"flow.in.resource_name": "/app/v1/request",
		"flow.in.payload_format": "json",

		"flow.out.resource_type": "rabbitmq",
		"flow.out.host": "rabbitmq",
		"flow.out.port": 5672,
		"flow.out.resource_name": "app-channel-pseudosync.request.app-in",
		"flow.out.payload_format": "json",
		"flow.out.ttl_seconds": 0,

		"flow.out.reply_to": "app-channel-pseudosync.response.app-out",

		"flow.message_dump": false,
		"flow.execute_services": false
	},
	{
		"channel": "app-channel-pseudosync-fake-rabbitmq",
		"_comment": "### Channel for PWBus Demo Pseudosync RabbitMQ",

		"engine.enabled": true,
		"engine.start_threads": 2,
		"engine.debug": true,

		"flow.in.resource_type": "rabbitmq",
		"flow.in.host": "rabbitmq",
		"flow.in.port": 5672,
		"flow.in.resource_name": "app-channel-pseudosync.request.app-in",
		"flow.in.payload_format": "json",

		"flow.out.resource_type": "rabbitmq",
		"flow.out.host": "rabbitmq",
		"flow.out.port": 5672,
		"flow.out.resource_name": "app-channel-pseudosync.response.app-out",
		"flow.out.payload_format": "json",

		"flow.message_dump": false,
		"flow.execute_services": true
	},

	{
		"_comment1": "----------------------------------------------------------",
		"_comment2": "### Channel for PWBus Demo AWS SQS",
		"_comment3": "----------------------------------------------------------",

		"channel": "app-channel-pseudosync-sqs",

		"engine.enabled": true,
		"engine.debug": true,

		"flow.in.resource_type": "http",
		"flow.in.resource_name": "/app/v1/request",
		"flow.in.payload_format": "json",

		"flow.out.resource_type": "sqs",
		"flow.out.resource_name": "https://sqs.us-east-1.amazonaws.com/408343843105/app-request-in",
		"flow.out.payload_format": "json",
		"flow.out.ttl_seconds": 0,

		"flow.out.reply_to": "https://sqs.us-east-1.amazonaws.com/<account-id>/app-response-out",

		"flow.message_dump": false,
		"flow.execute_services": false
	},
	{
		"channel": "app-channel-pseudosync-fake-sqs",
		"_comment": "### Channel for PWBus Demo Pseudosync AWS SQS",

		"engine.enabled": false,
		"engine.start_threads": 2,
		"engine.debug": true,

		"flow.in.resource_type": "sqs",
		"flow.in.resource_name": "https://sqs.us-east-1.amazonaws.com/<account-id>/app-request-in",
		"flow.in.payload_format": "json",

		"flow.out.resource_type": "sqs",
		"flow.out.resource_name": "https://sqs.us-east-1.amazonaws.com/<account-id>/app-response-out",
		"flow.out.payload_format": "json",

		"flow.message_dump": false,
		"flow.execute_services": true
	},

	{
		"_comment1": "----------------------------------------------------------",
		"_comment2": "### Channel for PWBus Demo Apache Kafka",
		"_comment3": "----------------------------------------------------------",

		"channel": "app-channel-pseudosync-kafka",

		"engine.enabled": true,
		"engine.debug": true,

		"flow.in.resource_type": "http",
		"flow.in.resource_name": "/app/v1/request",
		"flow.in.payload_format": "json",

		"flow.out.resource_type": "kafka",
		"flow.out.host": "kafka",
		"flow.out.port": 9092,
		"flow.out.resource_name": "app-request-in",
		"flow.out.payload_format": "json",
		"flow.out.ttl_seconds": 0,

		"flow.out.reply_to": "app-response-out",

		"flow.message_dump": false,
		"flow.execute_services": false
	},
	{
		"channel": "app-channel-pseudosync-fake-kafka",
		"_comment": "### Channel for PWBus Demo Pseudosync Apache Kafka",

		"engine.enabled": true,
		"_____________warning": "if great than 1 cause message out duplication",
		"engine.start_threads": 1,
		"engine.debug": true,

		"flow.in.resource_type": "kafka",
		"flow.in.host": "kafka",
		"flow.in.port": 9092,
		"flow.in.resource_name": "app-request-in",
		"flow.in.payload_format": "json",

		"flow.out.resource_type": "kafka",
		"flow.out.host": "kafka",
		"flow.out.port": 9092,
		"flow.out.resource_name": "app-response-out",
		"flow.out.payload_format": "json",

		"flow.message_dump": false,
		"flow.execute_services": true
	},

	{
		"_comment1": "----------------------------------------------------------",
		"_comment2": "### Channel for PWBus Demo Mongo",
		"_comment3": "----------------------------------------------------------",

		"channel": "app-channel-pseudosync-mongo",

		"engine.enabled": true,
		"engine.debug": true,

		"flow.in.resource_type": "http",
		"flow.in.resource_name": "/app/v1/request",
		"flow.in.payload_format": "json",

		"flow.out.resource_type": "mongo",
		"flow.out.host": "mongodb",
		"flow.out.port": 21017,
		"flow.out.resource_name": "pwbus_db.app_request_in",
		"flow.out.payload_format": "json",

		"flow.out.reply_to": "pwbus_db.app_response_out",

		"flow.message_dump": false,
		"flow.execute_services": false
	},
	{
		"channel": "app-channel-pseudosync-fake-mongo",
		"_comment": "### Channel for PWBus Demo Pseudosync Mongo",

		"engine.enabled": true,
		"engine.start_threads": 3,
		"engine.debug": true,

		"flow.in.resource_type": "mongo",
		"flow.in.host": "mongodb",
		"flow.in.port": 21017,
		"flow.in.resource_name": "pwbus_db.app_request_in",
		"flow.in.payload_format": "json",

		"flow.out.resource_type": "mongo",
		"flow.out.host": "mongodb",
		"flow.out.port": 21017,
		"flow.out.resource_name": "pwbus_db.app_response_out",
		"flow.out.payload_format": "json",

		"flow.message_dump": false,
		"flow.execute_services": true
	},

	{
		"_comment1": "----------------------------------------------------------",
		"_comment2": "### Channel for PWBus Demo Async Service Request",
		"_comment3": "----------------------------------------------------------",

		"channel": "app-channel-async",

		"engine.enabled": false,
		"engine.debug": true,

		"flow.in.resource_type": "http",
		"flow.in.resource_name": "/app/v1/request",
		"flow.in.payload_format": "json",
		"flow.in.pseudosync_mode": false,

		"flow.out.resource_type": "redis",
		"flow.out.host": "redis",
		"flow.out.port": 6379,
		"flow.out.resource_name": "app-channel-async.request.app-in",
		"flow.out.payload_format": "json",
		"flow.out.ttl_seconds": 10,
		"flow.out.pseudosync_mode": false,

		"flow.message_dump": true,
		"flow.execute_services": true
	},
	{
		"channel": "app-channel-async-process-csv",
		"_comment": "### Channel for PWBus Demo Async Service - Process to CSV",

		"engine.enabled": false,
		"engine.start_threads": 4,
		"engine.debug": true,

		"flow.in.resource_type": "redis",
		"flow.in.host": "redis",
		"flow.in.port": 6379,
		"flow.in.resource_name": "app-channel-async.request.app-in",
		"flow.in.payload_format": "json",
		"flow.in.pseudosync_mode": false,

		"flow.out.resource_type": "redis",
		"flow.out.host": "redis",
		"flow.out.port": 6379,
		"flow.out.resource_name": "app-channel-async.response.app-out",
		"flow.out.payload_format": "csv",
		"flow.out.ttl_seconds": 10,
		"flow.out.pseudosync_mode": false,

		"flow.message_dump": true,
		"flow.execute_services": false
	},
	{
		"channel": "app-channel-async-read-csv-send-http",
		"_comment": "### Channel for PWBus Demo Async Service - read-csv-send-http",

		"engine.enabled": false,
		"engine.start_threads": 4,
		"engine.debug": true,

		"flow.in.resource_type": "redis",
		"flow.in.host": "redis",
		"flow.in.port": 6379,
		"flow.in.resource_name": "app-channel-async.response.app-out",
		"flow.in.payload_format": "csv",
		"flow.in.pseudosync_mode": false,

		"flow.out.resource_type": "http",
		"flow.out.payload_format": "json",
		"flow.out.ttl_seconds": 10,
		"flow.out.pseudosync_mode": false,

		"flow.message_dump": true,
		"flow.execute_services": false
	}
]
```
