# merlion

An etcd-friendly TCP proxy, following the
["autopilot"]
(https://www.joyent.com/blog/app-centric-micro-orchestration) pattern
described by Casey Bisson at Joyent.  Reconfigures itself
dynamically when backends change.

## Cautionary note

**This project is conducted using README-Driven Development, or
something vaguely akin to it.  You should probably, therefore, treat
this document as a statement of how I wish things would be, rather
than as a claim that this is presently how they in fact are**

As of Dec 2016:

* Poking gently at it, it seems to work,  but it's never actually
been used for realz

* (On my machine, at least) listeners bind to the ipv6 wildcard
  address no matter what address you tell it to use

* health checks currently unimplemented

* logging ditto (logging may go away completely)

## Installation

1. `git clone http://github.com/telent/merlion`
1. `cd merlion && lein uberjar`

## Usage

### Configuration

Merlion uses the etcd store both for its own configuration and for
finding the details of the backend services it is proxying.  Choose an
etcd keyspace prefix for configuration for this instance/cluster
itself.  Under this prefix, merlion expects the following keys

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

* performance should be "adeqate to the task": this is intended to be
  used in front of some kind of dynamic web application server so
  ideally shouldn't be much slower than running that server directly.

* per the *autopilot pattern*, the responsibility of ensuring that a
  backend is healthy is the backend's: as long as it thrives, it
  should update its last-seen-at key frequently enough for merlion to
  believe it is still present. [ no, I don't know how this plays out
  with network partitions, I guess we'll find out ]

* Clojure-friendly but capable of use for backends in other languages
  without anyone having to learn the language



## License

Copyright © 2016,2017 Daniel Barlow

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
