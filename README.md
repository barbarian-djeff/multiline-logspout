Logspout multiline adapter
==========================

Implementation of a multiline adapter for Logspout.
For the moment, it handles spring boot stacktraces specifically and sends json event to Logstash on TCP by default.

The code is based on the github repo [anashaka/logspout-logstash](github.com/anashaka/logspout-logstash).