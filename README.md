# merlion

An etcd-friendly HTTP proxy, following the
["autopilot"]
(https://www.joyent.com/blog/app-centric-micro-orchestration) pattern
described by Casey Bisson at Joyent.  Reconfigures itself
dynamically when backends change.


## Installation

1. `git clone http://github.com/telent/merlion`
1. `cd merlion && lein uberjar`

## Usage

### Configuration

Merlion uses the etcd store both for its own configuration and for
finding the details of the backend services it is proxying.  Choose an etcd keyspace prefix for configuration for this
instance/cluster itself.  Under this prefix, merlion expects the following
keys

* `listen-address` (optional, defaults to *:8080)
* `upstream-service-etcd-prefix` e.g. `/service/sinatra/helloworld/`
* `upstream-freshness` -  timeout in seconds after which an upstream that has not recently published a `last-seen-at` timestamp is removed from service
* `log-format` - one of `none`, `json`, `edn` or `ncsa` (default `json`)
* [ stuff for performance and monitoring, tbd exactly what ]

### Invocation

    $ java -jar merlion-0.1.0-standalone.jar /the/etcd/path/to/merlion/config


### Registering upstreams

Merlion will probably do nothing interesting until you have some backends registered.  It expects each of the services it is proxying to have registered their details in etcd at the prefix specified by `upstream-service-etc-prefix`.  For example, if the prefix is  `/service/sinatra/helloworld/` you might have any/all of `/service/sinatra/helloworld/21345`, `/service/sinatra/helloworld/i-346ad1`, `/service/sinatra/helloworld/127_0_0_1/`

Each etcd directory node within the configured prefix is added as a valid service if the following conditions are met:

* the key `listen-address` (e.g. `localhost:4567`) is present and not obviously incorrect.  The upstream service should be listening at that address, otherwise things will tend to not work.

* the key `last-seen-at` is a valid ISO8166 datetime and the interval between `last-seen-at` and the time now is less than  `upstream-freshness` seconds

* the key `disabled` is not present

# Principles

* respond almost instantly to any changes in the etcd configuration

* do not drop "in-flight" requests on the frontend or backend when
  configuration changes

* performance should be "what you'd expect from netty" - as it's based
  on netty.

* Clojure-friendly but capable of use for backends in other languages
  without anyone having to learn the language



## License

Copyright © 2016 Daniel Barlow

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
