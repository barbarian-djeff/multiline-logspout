Logspout multiline adapter
==========================

Implementation of a multiline adapter for Logspout.
For the moment, it handles spring boot stacktraces specifically and sends json event to Logstash on TCP by default.

The code is based on the github repo [anashaka/logspout-logstash](github.com/anashaka/logspout-logstash).

Follow the instructions in https://github.com/gliderlabs/logspout/tree/master/custom on how to build your own Logspout container with custom modules.
