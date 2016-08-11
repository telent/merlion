(ns merlion.etcd
  (:require
   [clojure.test :as test :refer [is deftest with-test]]
   [ring.util.codec]
   [clojure.core.async :as async :refer [<! go chan >!]]
   [clojure.string :as str]
   [aleph.http :as http]
   [byte-streams :as bs]
   [cheshire.core :as json]
   [clojure.edn :as edn]))


(defn value-from-etcd-node [nodename node]
  (if (get node "dir")
    (reduce (fn [m e]
              (assoc
               m
               (keyword (subs (str/replace-first (get e "key") nodename "") 1))
               (value-from-etcd-node (get e "key") e)))
            {}
            (get node "nodes"))
    (get node "value")))

(defn map-from-etcd-response [prefix response-body]
  (let [j (json/decode response-body)]
    (if-let [node (get j "node")]
      (value-from-etcd-node prefix node)
      (with-meta {} {:error j}))))

(deftest map-from-etcd-response-test
  (let [body (slurp "test/fixtures/etcd-response.json")
        m (map-from-etcd-response  "/conf/dev.booksh.lv" body)]
    (is (= (keys m) [:auth]))
    (is (= (keys (:auth m)) [:google :facebook :yahoo]))
    (is (= (-> m :auth :google :client-id)
           "608620955125-9plvti1kpi8vjacjo3ssmei9m43r1qo9.apps.googleusercontent.com"))))

(def get-http
  (comp slurp :body deref #(http/get % {:throw-exceptions false})))
(def put-http
  (comp slurp :body deref #(http/get % {:throw-exceptions false})))
(def etcd-endpoint "http://localhost:2379/v2/keys")

(defn get-prefix [prefix]
  (->> (str etcd-endpoint prefix "/?recursive=1")
       get-http
       (map-from-etcd-response prefix)))

(defn watch-prefix [prefix]
  (let [ch (chan)]
    (go
      (loop []
        (>! ch (get-http (str etcd-endpoint prefix "/?wait=true&recursive=true")))
        (recur)))
    ch))


(defn put-value [path value]
  (let [nname (str/join "/" (map name path))]
    (json/decode
     (put-http (str etcd-endpoint "/"  nname)
               {:headers {:content-type "application/x-www-form-urlencoded"}

                :body (ring.util.codec/form-encode {:value value})}))))
