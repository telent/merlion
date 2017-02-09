(ns merlion.etcd
  (:require
   [clojure.test :as test :refer [is deftest with-test]]
   [ring.util.codec]
   [clojure.core.async :as async :refer [<! go chan >!]]
   [clojure.string :as str]
   [clj-http.lite.client :as http]
   [cheshire.core :as json]))


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
  (comp :body #(http/get % {:throw-exceptions false})))
(def put-http
  (comp :body #(http/put %1 (merge %2 {:throw-exceptions false}))))
(def etcd-endpoint "http://localhost:2379/v2/keys")

(defn get-prefix [prefix]
  (->> (str etcd-endpoint prefix "/?recursive=1")
       get-http
       (map-from-etcd-response prefix)))

(defn watch-prefix
  "Returns a core.async channel which delivers a val whenever an etcd key under `prefix` is added/deleted/changed."
  [prefix]
  (let [ch (chan)]
    (go
      (loop []
        (>! ch (get-http (str etcd-endpoint prefix "/?wait=true&recursive=true")))
        (recur)))
    ch))


(defn put-value [path value]
  (json/decode
   (put-http (str etcd-endpoint path)
             {:headers {"content-type" "application/x-www-form-urlencoded"}

              :body (ring.util.codec/form-encode {:value value})})))

(defn put-map [path value]
  (run!
   (fn [[k v]]
     (let [k (str path "/" (if (keyword? k) (name k) k))]
       (if (map? v)
         (put-map k v)
         (put-value k v))))
   value))
