(ns merlion.inet
  (:import (java.net InetAddress))
  (:require [clojure.string :as str]
            [clojure.test :as test :refer [deftest testing is]]
            [clojure.set :as set]
            [taoensso.timbre :as log]
            [merlion.spec :as spec :refer [coercer]]
            [clojure.spec :as s]))

(s/def ::address #(instance? java.net.InetAddress %))
(s/def ::port int?)
(s/def ::hostname string?)
(s/def ::socket-address (s/keys :req [::address ::port]))


(defn resolve-host [hostname]
  (java.net.InetAddress/getByName hostname))

(s/fdef resolve-host
        :args ::hostname
        :ret ::address)
