conf(){
    curl -XPUT -d value="$2" http://localhost:2379/v2/keys$1;
}

site=dev.example.com
prefix=/conf/merlion/$site

if [ "$1" = "clean" ] ; then
    etcdctl rm --recursive /conf
    etcdctl rm --recursive /service
    exit 0
fi

conf $prefix/upstream-service-etcd-prefix /service/$site
conf $prefix/upstream-freshness 60
conf $prefix/log-format edn
conf $prefix/listen-address localhost:8088

conf /service/$site/af1/listen-address "localhost:4567"
conf /service/$site/af1/last-seen-at "$(TZ=UTC date -Iseconds)"
conf /service/$site/foo2/listen-address "localhost:4568"
conf /service/$site/foo2/last-seen-at "$(TZ=UTC date -Iseconds)"
