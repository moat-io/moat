# Monitoring, Events & Webhooks

## Component Monitoring
The components of moat which should be monitored in a production environment:

* **Moat API Server:** HTTP errors, CPU consumption
* **Ingestion processes:** Kubernetes `CronJob` status
* **Postgres Database:** Performance and storage capacity


## Events in Moat
Moat supports several event types which can be pushed into another system for monitoring/audit.

### Event Management
![Event Management](../images/event_mgmt.png)

Any process which makes a change to data within moat will generate an event. These events are passed to the `Event Logger`
which is responsible for deliviering them according to the specifics of the event logger class. 

### Event Types
The event types currently supported are:

| Event Type   | Actions                        | Description                             |
|--------------|--------------------------------|-----------------------------------------|
| `principal`  | `create` `update` `delete`     | Principal creation, update and deletion |
| `resource`   | `create` `update` `delete`     | Resource creation, update and deletion  |
| `ingestion`  | `started` `completed` `failed` | Status of ingestion processes           |
| `opa`        | `status_update` `decision`     | OPA status reports and decisions        |
| `bundle`     | `rebuilt` `consumed`           | Actions performed on policy bundles     |

### Configuration
All event logger configuration items use the prefix: `event_logger.<type>` where the type matches the `NAME` field on the 
event logger subclass. Currently only `http` is available.


| Configuration Item   | Description                                          | Example |
|----------------------|------------------------------------------------------|---------|
| `event_logger.type`  | The value of the `NAME` feild on the logger subclass | `http`  |


### Available Event Loggers
#### HTTP Logger
The HTTP event logger is selected by configuring `event_logger.type: http`. It provides webhooks, sending `json` as 
a `POST` request. This logger is configured as follows:

| Configuration Item                   | Description                                                                                  | Example                                      |
|--------------------------------------|----------------------------------------------------------------------------------------------|----------------------------------------------|
| `event_logger.http.url`              | The fully qualified URL endpoint the webhook should be sent to                               | `https://example.com/webhook`                |
| `event_logger.http.headers`          | A list of headers to send as comma-seperated name=value pairs                                | `Api-Key=ABCD,content-type=application/json` |
| `event_logger.http.extra_args`       | Additional parameters to be added to the object delivered (comma-seperated name=value pairs) | `source=moat`                                |
| `event_logger.http.ssl_verify`       | Whether to verify the SSL certificate of the called API (default=true)                       | `false`                                      |
| `event_logger.http.flatten_payload`  | Whether to flatten nested json (default=false)                                               | `true`                                        |


### Integration
While the default implementation of the event logger is powerful, integrations with other systems can be tricky. For complex
integrations where data may need to be presented in a specific format, `fluentd` is recommended as a simple proxy. `fluentd` 
has a lot of available plugins which can sink events to databases, streams and other systems.
