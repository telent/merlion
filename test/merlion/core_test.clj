(ns merlion.core-test
  (:import [java.net InetAddress Socket])
  (:require [me.raynes.conch.low-level :as shell]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.core.async :as async]
            [merlion.core :as core]
            [clojure.test :as test :refer [deftest testing is]]
            ))

(defn socat [opts]
  (let [p (shell/proc "/usr/bin/env" "socat"
                      "-u" "-d" "-d"
                      (:source opts)
                      "tcp-listen:0")]
    (let [e (io/reader (:err p))]
      (loop [lines (line-seq e)]
        (let [l (first lines)]
          (if-let [m  (re-find #"N listening on AF=2 (.+):([0-9]+)" l)]
            (assoc p :port (Integer/parseInt (last m)))
            (recur (rest lines))))))))

(def port (if-let [p (System/getenv "TESTPORT")]
            (Integer/parseInt p)
            8088))

(def domain-name "test.merlion.telent.net")
(def prefix
  (let [n (.getName (java.lang.management.ManagementFactory/getRuntimeMXBean))]
    (str "/" (str/replace n #"[^a-zA-Z0-9]" "_"))))

(defn seed-etcd []
  (doseq
      [c [["etcdctl" "rm" "--recursive"
           (str prefix "/conf/merlion/" domain-name)]
          ["etcdctl" "set"
           (str prefix "/conf/merlion/" domain-name "/upstream-freshness")
           "3600"]
          ["etcdctl" "set"
           (str prefix "/conf/merlion/" domain-name "/upstream-service-etcd-prefix")
           (str prefix "/service/" domain-name)]
          ["etcdctl" "set"
           (str prefix "/conf/merlion/" domain-name "/listen-address")
           (str "localhost:" port)]]]
    (print (shell/stream-to-string (apply shell/proc c) :out))))

(def server (atom nil))

(defn setup-fixtures [f]
  (seed-etcd)
  (reset! server (core/run-server (str prefix "/conf/merlion/" domain-name)))
  (f)
  (async/put! @server :quit))

(test/use-fixtures :once setup-fixtures)

(defn etcdctl [name value]
  (let [c ["/usr/bin/env" "etcdctl" "set" (str prefix "/" name) value]]
    (print (shell/stream-to-string (apply shell/proc c) :out))))

(defn tcp-slurp [host port]
  (let [s (Socket. host port)]
    (slurp (io/reader (.getInputStream s)))))

(deftest download
  (testing "small file transfer"
    (let [be-process (socat {:source "file:test/fixtures/excerpt.txt"})]
      (etcdctl (str "service/" domain-name "/aaa/listen-address")
               (str "localhost:" (:port be-process)))
      (etcdctl (str "service/" domain-name "/aaa/last-seen-at")
               (.toString (java.time.Instant/now)))
      (Thread/sleep 500)
      (is (= (slurp "test/fixtures/excerpt.txt")
             (tcp-slurp (InetAddress/getLoopbackAddress) port)))
      )))
