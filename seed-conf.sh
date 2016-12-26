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
conf $prefix/upstream-freshness 3600
conf $prefix/log-format edn
conf $prefix/listen-address localhost:8087

conf /service/$site/afs1/listen-address "localhost:8023"
conf /service/$site/afs1/last-seen-at "$(TZ=UTC date -Iseconds| sed 's/+/%2b/' )"

conf /service/$site/afs2/listen-address "localhost:8024"
conf /service/$site/afs2/last-seen-at "$(TZ=UTC date -Iseconds| sed 's/+/%2b/' )"

