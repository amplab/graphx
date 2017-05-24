---
layout: global
title: Spark Security
---

Spark currently supports authentication via a shared secret. Authentication can be configured to be on via the `spark.authenticate` configuration parameter. This parameter controls whether the Spark communication protocols do authentication using the shared secret. This authentication is a basic handshake to make sure both sides have the same shared secret and are allowed to communicate. If the shared secret is not identical they will not be allowed to communicate.

The Spark UI can also be secured by using javax servlet filters. A user may want to secure the UI if it has data that other users should not be allowed to see. The javax servlet filter specified by the user can authenticate the user and then once the user is logged in, Spark can compare that user versus the view acls to make sure they are authorized to view the UI. The configs 'spark.ui.acls.enable' and 'spark.ui.view.acls' control the behavior of the acls. Note that the person who started the application always has view access to the UI.

For Spark on Yarn deployments, configuring `spark.authenticate` to true will automatically handle generating and distributing the shared secret. Each application will use a unique shared secret. The Spark UI uses the standard YARN web application proxy mechanism and will authenticate via any installed Hadoop filters. If an authentication filter is enabled, the acls controls can be used by control which users can via the Spark UI. 

For other types of Spark deployments, the spark config `spark.authenticate.secret` should be configured on each of the nodes. This secret will be used by all the Master/Workers and applications. The UI can be secured using a javax servlet filter installed via `spark.ui.filters`. If an authentication filter is enabled, the acls controls can be used by control which users can via the Spark UI.

IMPORTANT NOTE: The NettyBlockFetcherIterator is not secured so do not use netty for the shuffle is running with authentication on.

See [Spark Configuration](configuration.md) for more details on the security configs.

See <a href="api/core/index.md#org.apache.spark.SecurityManager"><code>org.apache.spark.SecurityManager</code></a> for implementation details about security.
