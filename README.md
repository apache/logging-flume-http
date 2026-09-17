<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Project status

> [!WARNING]
> **As of May 2026 this project is undergoing significant rework! We do not
> advise using it until it is restablized and a formal release is announced.**
> [It has been marked as dormant by Apache Logging Services consensus on 2024-10-10.](https://lists.apache.org/thread/dg9wro6dp7w95o1x911lbyqxzl808b3l)
> Users are advised to migrate to alternatives.
> For other inquiries, see [the support policy](https://logging.apache.org/support.html).

# Welcome to Apache Flume HTTP!

Apache Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of event data. It has a simple
and flexible architecture based on streaming data flows. It is robust and fault
tolerant with tunable reliability mechanisms and many failover and recovery
mechanisms. The system is centrally managed and allows for intelligent dynamic
management. It uses a simple extensible data model that allows for online
analytic application.

Apache Flume HTTP provides the components that let Flume agents exchange events
over HTTP:

* `flume-http-source`: a source that accepts Flume events over HTTP or HTTPS,
  with pluggable JSON and BLOB request handlers,
* `flume-http-sink`: a sink that forwards Flume events to an HTTP endpoint.

Apache Flume HTTP is open-sourced under the Apache Software Foundation License v2.0.

## Documentation

The Flume 2.x guide and FAQ are available here:

* https://flume.apache.org/

## Compiling Flume HTTP

Compiling Flume HTTP requires Java 17 or later; the Maven wrapper (`./mvnw`) downloads the right Maven version.

## Contact us!

* Mailing lists: https://logging.apache.org/support.html
* Slack channel #flume on https://the-asf.slack.com/

Bug and Issue tracker.

* https://github.com/apache/logging-flume-http/issues
