(ns merlion.core-test
  (:import [java.net InetAddress Socket])
  (:require [me.raynes.conch.low-level :as shell]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.core.async :as async]
            [merlion.core :as core]
            [taoensso.timbre :as log :refer [debug info trace]]
            [clojure.test :as test :refer [deftest testing is]]
            ))

(log/set-level! (if-let [l (System/getenv "LOGLEVEL")] (keyword l) :info))

(def excerpt-txt (slurp "test/fixtures/excerpt.txt"))

(defn read-process-out-for-port [p]
  (let [e (io/reader (:err p))]
    (loop [lines (line-seq e)]
      (let [l (first lines)]
        (trace l)
        (if-let [m  (re-find #"N listening on AF=2 (.+):([0-9]+)" l)]
          (assoc p :port (Integer/parseInt (last m)))
          (recur (rest lines)))))))

(defn socat-pipe [command]
  (let [p (shell/proc "/usr/bin/env"
                      "sh" "-c"
                      (str command " | socat -u -d -d stdin tcp-listen:0,reuseaddr"))]
    (read-process-out-for-port p)))


(defn socat-file-server [filename]
  (let [p (shell/proc "/usr/bin/env"
                      "sh" "-c"
                      (str "socat  -d -d tcp-listen:0,reuseaddr,fork file:" filename))]
    (read-process-out-for-port p)))


(defn socat-pipe-with-port [command port]
  (let [p (shell/proc "/usr/bin/env"
                      "sh" "-c"
                      (str command " | socat -u -d -d stdin tcp-listen:" port ",reuseaddr"))]
    (let [e (io/reader (:err p))]
      (loop [lines (line-seq e)]
        (let [l (first lines)]
          (trace l)
          (if-let [m  (re-find #"N listening on AF=2 (.+):([0-9]+)" l)]
            (assoc p :port (Integer/parseInt (last m)))
            (recur (rest lines))))))))

(defn socat [filename]
  (socat-pipe (str "cat " filename)))

(defn slow-socat [filename bps]
  (socat-pipe (str "pv -q -L " bps " " filename)))

(def port (if-let [p (System/getenv "PORT")]
            (Integer/parseInt p)
            8095))

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


(defn call-with-running-server [f]
  (seed-etcd)
  (let [shutdown (async/chan)
        server (core/run-server (str prefix "/conf/merlion/" domain-name)
                                shutdown)]
    (try
      (f)
      (finally
        (async/put! shutdown :quit)
        (log/spy (async/<!! server))
        (let [c ["etcdctl" "rm" "--recursive" prefix]]
          (shell/stream-to-string (apply shell/proc c) :out))))))

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


(defn add-backend
  ([name address port]
   (etcdctl (str "service/" domain-name "/" name "/listen-address")
            (str address ":" port))
   (etcdctl (str "service/" domain-name "/" name "/last-seen-at")
            (.toString (java.time.Instant/now))))
  ([name port]
   (add-backend name "localhost" port)))


(deftest download
  (testing "small file transfer"
    (let [be-process (socat "test/fixtures/excerpt.txt")]
      (with-running-server [prefix port]
        (add-backend "aaa" (:port be-process))
        (Thread/sleep 500)
        (is (= excerpt-txt
               (tcp-slurp port)))))))


(deftest slow-download
  (testing "slow file transfer"
    (let [be-process (slow-socat "test/fixtures/excerpt.txt" 100)]
      (with-running-server [prefix port]
        (add-backend "aaa" (:port be-process))
        (Thread/sleep 500)
        (is (= excerpt-txt
               (tcp-slurp port)))
        ))))


(deftest quit-server
  (testing "quitting the server does not interrupt transfer"
    (seed-etcd)
    (let [be-process (slow-socat "test/fixtures/excerpt.txt" 100)
          shutdown (async/chan)
          server (core/run-server (str prefix "/conf/merlion/" domain-name)
                                  shutdown)]
      (future (Thread/sleep 4000) (async/put! shutdown :quit))
      (add-backend "aaa" (:port be-process))
      (Thread/sleep 500)
      (is (= excerpt-txt (tcp-slurp port)))
      (log/spy (async/<!! server)))))

(deftest quit-backend
  (testing "deleting the backend does not interrupt transfer"
    (let [be-process (slow-socat "test/fixtures/excerpt.txt" 100)]
      (with-running-server [prefix port]
        (add-backend "aaa" (:port be-process))
        (Thread/sleep 500)
        (let [f (future (tcp-slurp port))]
          (Thread/sleep 4000)
          (etcd-rm (str "service/" domain-name "/aaa"))
          (is (= excerpt-txt @f)))))))

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
          (is (= excerpt-txt (tcp-slurp new-port))))))))

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
            (is (= excerpt-txt @f))))))))

(deftest error-when-backend-down
  (testing "a broken upstream kills the backend"
    (with-running-server [prefix port]
      (let [be-process (socat "/dev/null")
            ;; We need a port number in which there is nothing listening.
            ;; Unless the machine is ridiculously busy, it should be
            ;; reasonably OK to start a server, consume the stream
            ;; before we start the test, and hope the port
            ;; is not re-allocated
            _ (tcp-slurp (:port be-process))]
        (add-backend "aaa" (:port be-process))
        (Thread/sleep 500)
        ;; we don't actually have a strong requirement for what happens while
        ;; the upstream is down, but let's at least check it's down
        (is (= "" (try (tcp-slurp port) (catch Exception e ""))))
        ;; but whatever it was, the server should carry on responding in the same
        ;; way when we restart the upstream and check that
        (let [new-process (socat-pipe-with-port
                           "cat test/fixtures/excerpt.txt"
                           (:port be-process))]
          (Thread/sleep 500)
          (is (= "" (try (tcp-slurp port) (catch Exception e "")))))))))

(deftest etcd-state-when-backend-down
  (testing "we can see in etcd when a backend minder exits"
    (with-running-server [prefix port]
      (let [be-process (socat "/dev/null")
            _ (tcp-slurp (:port be-process))]
        (etcdctl (str "/conf/merlion/" domain-name "/state-etcd-prefix")
                 (str prefix "/state"))
        (add-backend "aaa" (:port be-process))
        (Thread/sleep 500)
        (is (= "" (try (tcp-slurp port) (catch Exception e ""))))
        (Thread/sleep 50)               ;much discomfort
        (let [be-state
              (->
               (merlion.etcd/get-prefix  (str prefix "/state"))
               :backends :aaa)
              ls (Long/parseLong (:last-seen-at be-state))
              xt (Long/parseLong (:exited-at be-state))]
          (is (> xt ls)))))))

(deftest excommunicate-stale-backend
  (testing "we don't send requests to backends that haven't checked in recently"
    (with-running-server [prefix port]
      (let [be1 (socat-file-server "test/fixtures/excerpt.txt")
            be2 (socat-file-server "test/fixtures/excerpt2.txt")
            t1 excerpt-txt
            t2 (slurp "test/fixtures/excerpt2.txt")
            config-prefix (str "/conf/merlion/" domain-name "/")]
        (etcdctl (str config-prefix "state-etcd-prefix")
                 (str prefix "/state"))
        (etcdctl (str config-prefix "upstream-freshness") "5")
        (add-backend "aaa" (:port be1))
        (add-backend "bbb" (:port be2))
        (Thread/sleep 3000)
        (add-backend "bbb" (:port be2))
        (Thread/sleep 3000)
        ;;; bbb should now be the only backend remaining.
        ;;; of course, it's *possible* this test could succeed by accident
        (dotimes [i 10]
          (is (= t2 (tcp-slurp port)))
          (Thread/sleep 100))))))

(deftest exit-when-no-config
  (testing "server quits unless there is a minimal correct config in etcd"
    (doseq
        [c [["etcdctl" "rm" "--recursive" prefix]
            ]]
      (shell/stream-to-string (apply shell/proc c) :out))
    (let [c (async/chan)
          tm (async/timeout 1000)
          s (core/run-server (str prefix "/conf/merlion/" domain-name) c)]
      (is (= (second (async/alts!! [s tm])) s)))))


;;# close frontend when there are no backends?
;;#
