conf(){
    curl -XPUT -d value="$2" http://localhost:2379/v2/keys$1;
}
fconf(){
    curl  -XPUT --data-urlencode $2 http://localhost:2379/v2/keys$1;
}

site=dev.example.com
prefix=/conf/merlion/$site

if [ "$1" = "clean" ] ; then
   etcdctl rm --recursive /conf 
fi

conf $prefix/upstream-service-etcd-prefix /service/$site
conf $prefix/upstream-freshness 60
conf $prefix/log-format edn
fconf $prefix/tls-certificate value@test-ssl.crt
fconf $prefix/tls-private-key value@test-ssl.key
