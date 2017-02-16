(ns merlion.config
  (:require [clojure.string :as str]
            [clojure.test :as test :refer [deftest testing is]]
            [clojure.set :as set]
            [taoensso.timbre :as log]
            [merlion.spec :as spec :refer [coercer]]
            [clojure.spec :as s]))

(defn s->millepoch-time [s]
  (.getTimeInMillis (clojure.instant/read-instant-calendar s)))

(s/def ::host string?)
(s/def ::port int?)

(defn parse-address [a]
  (let [[host port] (str/split a #":")]
    {::host host ::port (Integer/parseInt port)}))

(s/def ::address (coercer (s/keys :req [::host ::port]) parse-address))

(s/conform ::address (s/conform ::address "sgadfgfdg:123" ))

(s/def ::timestamp (coercer int? s->millepoch-time))
(s/conform ::timestamp (s/conform ::timestamp "2017-01-25T14:20:33+00:00"))
(s/def ::listen-address ::address)
(s/def ::last-seen-at ::timestamp)

(defn trueish? [v]
  (contains? #{true 1 "1" "true" "t" "yes" "y"} v))

(s/def ::boolean (coercer boolean? trueish?))
(s/def ::disabled ::boolean)
(s/def ::backend (s/keys :req-un [::listen-address]
                         :opt-un [::last-seen-at ::disabled]))

(deftest conformations
  (is (s/conform ::boolean true))
  (is (s/conform ::boolean "true"))
  (is (s/conform ::boolean "false"))
  (is (s/conform ::backend {:listen-address "localhost:8192"
                            :last-seen-at "2017-01-25T14:20:33+00:00"
                            :disabled "true"}))

  (s/conform ::listen-address "localhost:8192"))

;(s/def ::backends (s/coll-of ::backend))

(s/def ::upstream-service-etcd-prefix string?)
(s/def ::state-etcd-prefix string?)
(s/def ::upstream-freshness (coercer number? #(Long/parseLong %)))
(defn level-from-string [s]
  (log/valid-level (keyword s)))

(s/def ::log-level (coercer log/valid-level? level-from-string))

(log/valid-level (keyword "trace"))
(s/conform ::log-level :traces)
(s/conform ::log-level :trace)
(s/conform ::log-level "traces")

(s/def ::config
  (s/keys :req-un [::upstream-service-etcd-prefix]
          :opt-un [::state-etcd-prefix
                   ::listen-address
                   ::log-level
                   ::upstream-freshness]))

(defn with-defaults [config]
  (merge
   {:upstream-freshness (* 1000 300)
    :listen-address "0.0.0.0:8080"}
   config))
