# Merlion

[![CircleCI](https://circleci.com/gh/telent/merlion.svg?style=svg)](https://circleci.com/gh/telent/merlion)

A TCP proxy configured via ["etcd"] (https://github.com/coreos/etcd) ,
following the ["autopilot"]
(https://www.joyent.com/blog/app-centric-micro-orchestration) pattern
described by Casey Bisson at Joyent.  Reconfigures itself dynamically
when backends change, to make zero-downtime code deployments simpler.


## Problem description

When deploying new versions of a web service/application, there are
three options generally considered:

* stop the old process, start the new one.  Simple and reliable, but
  involves downtime which may be significant if the new process takes
  a while to boot up.  In some environments (when traffic is high or
  if visitors are driven to the site with expensive pay-per-click
  adverts) this may cost a lot of money.

* "in-place" upgrade, using some kind of OS magic: for example, the
  nginx/unicorn trick where the old process is signalled to re-exec 
  its binary in such a way that the new process inherits the server
  socket from the old one.  This enables deployment without dropping
  connections, but IME is very easy to get wrong (e.g. inadvertently
  inheriting and sharing database connections) and can give unexpected
  errors if you don't understand the internals (e.g. expecting the new
  process to pick up environment variable settings made in the shell
  that signalled the restart - it won't).

* using a load balancer to which the new process can be added then the
  old one removed once we're happy that the new one is working.  This
  provides zero-downtime restarts without quite as many opportunities
  for unpleasant surprises as re-exec-in-place, at the expense of
  using double the system resources to run the old and new processes
  simultaneously, and the complexity of orchestrating the load
  balancer changes.
  
Merlion is my offering towards making option 3 manageable/automatable.
It provides a simple TCP load balancer which is dynamically
reconfigurable using etcd, allowing backends to be added or removed
during a deploy/rollback without the need to edit any files or restart
any load balancer processes.


## Cautionary note

**This project is conducted using README-Driven Development, or
something vaguely akin to it.  You should probably, therefore, treat
this document as a statement of how I wish things would be, rather
than as a claim that this is presently how they in fact are**

As of February 2017

* Poking gently at it, it seems to work,  but it's never actually
been used for realz

## Quick start/demonstration

You should usually be able to download a prebuilt Jar file
from [Github releases](https://github.com/telent/merlion/releases)
(these are built by CircleCI on each commit to `master`).  Alternatively, to build it manually (requires
Leiningen) do

```sh
# download and build the application
git clone http://github.com/telent/merlion`
cd merlion && lein uberjar
cp target/uberjar/merlion-*-standalone.jar .
```

Now you can configure and run it:


```
# Add Merlion config to etcd, using a namespace prefix of your choice
# (which should be unique to this service)
etcdctl set /conf/merlion/mysite.example.com/upstream-service-etcd-prefix /services/merlion
etcdctl set /conf/merlion/mysite.example.com/listen-address 0.0.0.0:8080

# Start it up (adapt version number as needed)
java -jar merlion-0.1.93-standalone.jar /conf/merlion/mysite.example.com

# It won't actually be able to do anything without a backend though.
# In a second terminal, run
python -m SimpleHTTPServer 8023 . 

# Now add the backend details to etcd
etcdctl set /services/merlion/mybackend/listen-address localhost:8023
etcdctl set /services/merlion/mybackend/last-seen-at "$(TZ=UTC date -Iseconds| sed 's/+/%2b/' )"

# and see it work
curl http://localhost:8080/README.md

```

## Usage

### Configuration

Merlion uses the etcd store both for its own configuration and for
finding the details of the backend services it is proxying.  Choose an
etcd keyspace prefix for configuration for this instance/cluster
itself.  Under this prefix, merlion expects the following keys

[ The list in this README is indicative, not definitive: for the definitive list,
see the "spec" for `:merlion.config/config` as described in
[src/merlion/config.clj]
(https://github.com/telent/merlion/blob/master/src/merlion/config.clj) ]

* `upstream-service-etcd-prefix` e.g. `/service/sinatra/helloworld/`
* `state-etcd-prefix` - a key space under which merlion maintains some
  state attributes of the service and its backends.  An external
  process watching this prefix would be able to
  tell if a backend had become unreachable, for example.  Optional,
  no default
* `listen-address` (optional, defaults to *:8080)
* `upstream-freshness` - how long can a backend go without "checking
  in" before we mark it unavailable.  Specifically, the timeout in
  seconds after which an upstream that has not recently published a
  `last-seen-at` timestamp is removed from service.  Optional,
  defaults to 300 (5 minutes)

### Invocation

    $ java -jar merlion-0.1.0-standalone.jar /the/etcd/path/to/merlion/config


### Registering upstreams

Merlion should do nothing interesting until you have some backends
registered.  It expects each of the services it is proxying to have
registered their details in etcd at the prefix specified by
`upstream-service-etc-prefix`.  For example, if the prefix is
`/service/sinatra/helloworld/` you might have any/all of
`/service/sinatra/helloworld/21345`,
`/service/sinatra/helloworld/i-346ad1`,
`/service/sinatra/helloworld/127_0_0_1/`

Each etcd directory node within the configured prefix is added as a
valid backend service if the following conditions are met:

* the key `listen-address` (e.g. `localhost:4567`) is present and not obviously incorrect.  The upstream service should be listening at that address, otherwise things will tend to not work.

* the key `last-seen-at` is a valid ISO8166 datetime and the interval between `last-seen-at` and the time now is less than `upstream-freshness` seconds

* the key `disabled` is not present

[ Again, these keys are indicative not definitive: for the definitive list,
see the "spec" for `:merlion.config/backend` as described in
[src/merlion/config.clj]
(https://github.com/telent/merlion/blob/master/src/merlion/config.clj) ]



# Design principles, priorities

* respond almost instantly to any changes in the etcd configuration

* do not drop "in-flight" requests on the frontend or backend when
  configuration changes

* per the *autopilot pattern*, the entity best placed to know whether
  some system or process is healthy is that system or process itself.
  Thus it is the responsibility of each backend to self-check its own
  status: as long as it thrives, it should update its last-seen-at key
  frequently enough for merlion to believe it is still present.

* monitoring/logging that can easily be integrated with the rest of
  your stack (file-based or ELK or syslog, nagios, ???) 

* performance should be "adeqate to the task": this is intended to be
  used in front of some kind of dynamic web application server so
  ideally shouldn't be much slower than running that server directly.

* Clojure-friendly, but capable of use for backends in other languages
  without anyone having to learn the language




## License

Copyright © 2016,2017 Daniel Barlow

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
