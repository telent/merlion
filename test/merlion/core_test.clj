(ns merlion.core-test
  (:import [java.net InetAddress Socket])
  (:require [me.raynes.conch.low-level :as shell]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.core.async :as async]
            [merlion.core :as core]
            [clojure.test :as test :refer [deftest testing is]]
            ))

(defn socat-pipe [command]
  (let [p (shell/proc "/usr/bin/env"
                      "sh" "-c"
                      (str command " | socat -u -d -d stdin tcp-listen:0"))]
    (let [e (io/reader (:err p))]
      (loop [lines (line-seq e)]
        (let [l (first lines)]
          (if-let [m  (re-find #"N listening on AF=2 (.+):([0-9]+)" l)]
            (assoc p :port (Integer/parseInt (last m)))
            (recur (rest lines))))))))

(defn socat [filename]
  (socat-pipe (str "cat " filename)))

(defn slow-socat [filename bps]
  (socat-pipe (str "pv -q -L " bps " " filename)))


(def port (if-let [p (System/getenv "PORT")]
            (Integer/parseInt p)
            8087))

(def domain-name "test.merlion.telent.net")
(def prefix
  (let [n (.getName (java.lang.management.ManagementFactory/getRuntimeMXBean))]
    (str "/" (str/replace n #"[^a-zA-Z0-9]" "_"))))

(defn seed-etcd []
  (doseq
      [c [["etcdctl" "rm" "--recursive" prefix]
          ["etcdctl" "set"
           (str prefix "/conf/merlion/" domain-name "/upstream-freshness")
           "3600"]
          ["etcdctl" "set"
           (str prefix "/conf/merlion/" domain-name "/upstream-service-etcd-prefix")
           (str prefix "/service/" domain-name)]
          ["etcdctl" "set"
           (str prefix "/conf/merlion/" domain-name "/listen-address")
           (str "localhost:" port)]]]
    (shell/stream-to-string (apply shell/proc c) :out)))

(def server (atom nil))

(defn call-with-running-server [f]
  (seed-etcd)
  (reset! server (core/run-server (str prefix "/conf/merlion/" domain-name)))
  (try
    (f)
    (finally
      (async/put! @server :quit)
      (let [c ["etcdctl" "rm" "--recursive" prefix]]
        (shell/stream-to-string (apply shell/proc c) :out)))))

(defmacro with-running-server [[pre prt] & body]
  `(call-with-running-server (fn [] (let [~pre prefix ~prt port]  ~@body))))

(defn etcdctl [name value]
  (let [c ["/usr/bin/env" "etcdctl" "set" (str prefix "/" name) value]]
    (shell/stream-to-string (apply shell/proc c) :out)))

(defn etcd-rm [name]
  (let [c ["/usr/bin/env" "etcdctl" "rm" "--recursive" (str prefix "/" name) ]]
    (shell/stream-to-string (apply shell/proc c) :out)))

(defn tcp-slurp [port]
  (let [s (Socket. (InetAddress/getLoopbackAddress) port)]
    (.setSoTimeout s 1000)
    (slurp (io/reader (.getInputStream s)))))

(defn add-backend [name port]
  (etcdctl (str "service/" domain-name "/" name "/listen-address")
           (str "localhost:" port))
  (etcdctl (str "service/" domain-name "/" name "/last-seen-at")
           (.toString (java.time.Instant/now))))


(deftest download
  (testing "small file transfer"
    (let [be-process (socat "test/fixtures/excerpt.txt")]
      (with-running-server [prefix port]
        (add-backend "aaa" (:port be-process))
        (Thread/sleep 500)
        (is (= (slurp "test/fixtures/excerpt.txt")
               (tcp-slurp port)))))))


(deftest slow-download
  (testing "slow file transfer"
    (let [be-process (slow-socat "test/fixtures/excerpt.txt" 100)]
      (with-running-server [prefix port]
        (add-backend "aaa" (:port be-process))
        (Thread/sleep 500)
        (is (= (slurp "test/fixtures/excerpt.txt")
               (tcp-slurp port)))
        ))))


(deftest quit-server
  (testing "quitting the server does not interrupt transfer"
    (let [be-process (slow-socat "test/fixtures/excerpt.txt" 100)]
      (let [fut
            (with-running-server [prefix port]
              (add-backend "aaa" (:port be-process))
              (Thread/sleep 500)
              (let [f (future (tcp-slurp port))]
                (Thread/sleep 4000)
                f))]
        (async/put! @server :quit)
        (is (= (slurp "test/fixtures/excerpt.txt") @fut))
        ))))

(deftest quit-backend
  (testing "deleting the backend does not interrupt transfer"
    (let [be-process (slow-socat "test/fixtures/excerpt.txt" 100)]
      (with-running-server [prefix port]
        (add-backend "aaa" (:port be-process))
        (Thread/sleep 500)
        (let [f (future (tcp-slurp port))]
          (Thread/sleep 4000)
          (etcd-rm (str "service/" domain-name "/aaa"))
          (is (= (slurp "test/fixtures/excerpt.txt") @f)))))))

(deftest no-backend
  (testing "fetches hang when no backends"
    (let [be-process (socat "test/fixtures/excerpt.txt")]
      (with-running-server [prefix port]
        (Thread/sleep 500)
        (is (thrown?
             java.net.SocketTimeoutException
             (tcp-slurp port)))))))


(deftest delete-backend
  (testing "deleting a backend makes it stop responding"
    (let [be-process (socat "test/fixtures/excerpt.txt")]
      (with-running-server [prefix port]
        (add-backend "aaa" (:port be-process))
        (Thread/sleep 500)
        (etcd-rm (str "service/" domain-name "/aaa"))
        (Thread/sleep 500)
        (is (thrown?
             java.net.SocketTimeoutException
             (tcp-slurp port)))
        ))))

(deftest change-listener
  (testing "the listening port can be changed"
    (let [be-process (socat "test/fixtures/excerpt.txt")
          p (:port be-process)]
      (with-running-server [prefix port]
        (let [new-port (inc port)]
          (add-backend "aaa" p)
          (Thread/sleep 500)
          (etcdctl (str "/conf/merlion/" domain-name "/listen-address")
                   (str "localhost:" new-port))
          (Thread/sleep 500)
          (is (= (slurp "test/fixtures/excerpt.txt") (tcp-slurp new-port))))))))





(deftest change-listener-no-interruption
  (testing "changing the listening port does not break existing transfers"
    (let [be-process (slow-socat "test/fixtures/excerpt.txt" 100)
          p (:port be-process)]
      (with-running-server [prefix port]
        (let [new-port (inc port)]
          (add-backend "aaa" p)
          (Thread/sleep 500)
          (let [f (future (tcp-slurp port))]
            (Thread/sleep 2000)
            (etcdctl (str "/conf/merlion/" domain-name "/listen-address")
                     (str "localhost:" new-port))
            (Thread/sleep 2000)
            (is (= (slurp "test/fixtures/excerpt.txt") @f))))))))
