(ns merlion.core
  (:import [java.nio Buffer ByteBuffer]
           [java.net InetSocketAddress]
           [java.nio.channels SocketChannel ServerSocketChannel
            Selector SelectionKey])
  (:require [merlion.etcd :as etcd]
            [clojure.string :as str]
            [clojure.test :as test :refer [deftest testing is]]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [clojure.core.async :as async
             :refer [<! <!! go chan >! close! alts!]]))

(defn s->millepoch-time [s]
  (.getTimeInMillis (clojure.instant/read-instant-calendar s)))

(defn millepoch-time-now []
  (.getTime (java.util.Date.)))


(defn server-socket
  "Opens a ServerSocketChannel listening on the specified TCP `port` then
repeatedly accepts a connection and sends it to the first chan from
`backends` to become ready"
  [port backends]
  (let [ssc (ServerSocketChannel/open)
        addr (InetSocketAddress. port)]
    (.. ssc socket (bind addr))
    (go (loop []
          (let [sock (.accept ssc)
                ports (map #(vector % sock) backends)]
            (if (first (alts! ports))
              (recur)
              ))))))

(defn copy-channel [in out buffer]
  (let [bytes (.read in buffer)]
    (when (> bytes -1)
      (.flip buffer)
      (.write out buffer)
      (.clear buffer)
      true)))


(defn write-pending-buffer [w]
  (let [ch (:to-channel w)
        b (:buffer w)]
    (.write ch b)
    (if (.hasRemaining b) w nil)))

(defn read-pending-buffer [from-channel to-channel]
  (let [b (ByteBuffer/allocate 8192)
        c (.read from-channel b)]
    (cond
      (zero? c) nil
      (< c 0) {:eof? true
               :from-channel from-channel
               :to-channel to-channel}
      :else {:to-channel to-channel :buffer (.flip b)})))

(defn poll-ops-mask [ops]
  (reduce + (map {:read SelectionKey/OP_READ
                  :write SelectionKey/OP_WRITE
                  :accept SelectionKey/OP_ACCEPT}
                 ops)))

(defn poll-op? [actual wanted]
  (not (zero? (bit-and actual (poll-ops-mask wanted)))))


(defn ready-for? [ops channel wanted]
  (first (filter (fn [s]
                   (and (= (:channel s) channel) (poll-op? (:ops s) [wanted])))
                 ops)))

(deftest poll-ops-test
  (let [rw (+ SelectionKey/OP_READ SelectionKey/OP_WRITE)]
    (is (= (poll-ops-mask [:read :write]) rw))
    (is (thrown? Exception (poll-ops-mask [:read :wrong])))

    (is (ready-for?
         [{:channel :c1 :ops rw}
          {:channel :c2 :ops (+ SelectionKey/OP_READ)}]
         :c1
         :write))
    (is (ready-for?
         [{:channel :c1 :ops rw}
          {:channel :c2 :ops (+ SelectionKey/OP_READ)}]
         :c2
         :read))
    (is (not (ready-for?
              [{:channel :c1 :ops rw}
               {:channel :c2 :ops (+ SelectionKey/OP_READ)}]
              :c2
              :write)))))


(defn poll-channels [poll-set timeout]
  (with-open [bo (reduce (fn [s [channel & ops]]
                           (.register channel s (poll-ops-mask ops))
                           s)
                         (Selector/open)
                         poll-set)]
    (if-not (zero? (.select bo timeout))
      (let [ready (.selectedKeys bo)]
        (doall ; have to force evaluation of this before closing selector
         (map (fn [k]
                {:channel (.channel k)
                 :ops (.readyOps k)})
              ready))))))

(deftest poll-test
  (testing "timeout"
    (let [before (millepoch-time-now)]
      (poll-channels [] 1000)
      (is (> (- (millepoch-time-now) before) 990))))

  (testing "accept"
    (let [ss (doto (ServerSocketChannel/open)
               (.configureBlocking false)
               (.bind (InetSocketAddress. 0)))
          c (doto (SocketChannel/open)
              (.connect
               (InetSocketAddress. "127.0.0.1" (.getLocalPort (.socket ss))))
              (.configureBlocking false))]
      (let [ready (poll-channels [[ss :accept]] 1000)]
        (is (ready-for? ready ss :accept)))))

  (testing "read and write"
    (with-open [ssh (doto (SocketChannel/open)
                      (.connect (InetSocketAddress. "127.0.0.1" 22))
                      (.configureBlocking false))]
      (let [ready (poll-channels [[ssh :write ]] 1000)]
        (is (ready-for? ready ssh :write)))
      (let [msg (ByteBuffer/wrap (.getBytes "hello world\n"))]
        (.write ssh msg)
        (is (ready-for? (poll-channels [[ssh :read]] 1000) ssh :read))))))

(defn backend [serversocket host port finished-ch]
  (loop [pending-write false
         downstreams-for-upstreams {}]
    ;; BACKPRESSURE - we do things in a very specific order.  This is
    ;; to ensure that we do not look at any avenues for accepting new
    ;; work (data
    ;; available on client connections, or new client connections) until
    ;; we have dealt with current work, and thus that if the downstream
    ;; is slow to accept our writes or slow to read from, we become
    ;; slower as a result instead of spraying it with even more work it
    ;; can't cope with.
    (let [downstreams (set (vals downstreams-for-upstreams))
          upstreams (set (keys downstreams-for-upstreams))
          upstreams-for-downstreams
          (set/map-invert downstreams-for-upstreams)
          poll-set
          (if pending-write
            [[(:to-channel pending-write) :write]]
            (concat (map #(vector % :read ) downstreams)
                    (map #(vector % :read ) upstreams)
                    [[serversocket :accept]]))]
      (let [ready (poll-channels poll-set 1000)
            ready-channels (set (map :channel ready))
            readable-ds
            (first (filter (fn [s]
                             (and (poll-op? (:ops s) [:read])
                                  (get downstreams (:channel s))))
                           ready))
            readable-us
            (first (filter (fn [s]
                             (and (poll-op? (:ops s) [:read])
                                  (get upstreams (:channel s))))
                           ready))
            ]
        (cond
          ;; Quit when we get a finished message
          (async/poll! finished-ch)
          nil

          ;; Zeroth, close the corresponding down/upstream for any up/downstream
          ;; we received eof while reading
          (and pending-write (:eof? pending-write))
          (let [fch (:from-channel pending-write)
                tch (:to-channel pending-write)]
            (.close fch)
            (.close tch)
            (recur nil (dissoc downstreams-for-upstreams fch tch)))

          ;; First deal with any unwritten buffer to/from the downstream.  We do not
          ;; get past this point until the in-flight buffer is disposed of

          pending-write
          (let [r (ready-for? ready (:to-channel pending-write) :write)]
            (if r
              (recur (write-pending-buffer pending-write)
                     downstreams-for-upstreams)
              (recur pending-write
                     downstreams-for-upstreams)))

          ;; Next deal with readable downstreams
          readable-ds
          (do
            (let [c (:channel readable-ds)]
              (recur (read-pending-buffer c (upstreams-for-downstreams c))
                     downstreams-for-upstreams)))

          ;; Next look at new work from existing upstream requests
          readable-us
          (do
            (let [c (:channel readable-us)]
              (recur (read-pending-buffer c
                                          (downstreams-for-upstreams c))
                     downstreams-for-upstreams)))

          ;; We can pick up a new upstream request if nothing else to do.
          ;; Note this may block in connect(), I haven't decided if that's
          ;; bad or not
          (get ready-channels serversocket)
          (let [sock (doto (.accept serversocket)
                       (.configureBlocking false))]
            (recur nil
                   (assoc downstreams-for-upstreams
                          sock
                          (doto (SocketChannel/open)
                            (.connect (InetSocketAddress. host port))
                            (.configureBlocking false)))))
          :default
          (do
            (println "timeout")
            (recur pending-write downstreams-for-upstreams))


          )))))


(defn backend-chan
  [host port listener]
  (println ["backend chan for " host port])
  (let [ch (chan)]
    (future
      (backend listener host port ch)
      (println ["backend thread quit" host port]))
    ch))

(defn parse-address [a]
  (let [[address port] (str/split a #":")]
    {:address address :port (Integer/parseInt port)}))

(defn update-backends [backends config listener]
  (let [existing (keys backends)
        wanted (keys (:backends config))
        unwanted (set/difference (set existing) (set wanted))]
    (println ["unwanted " unwanted])
    (run! close! (map #(:chan (get backends %)) unwanted))
    (reduce (fn [m [n v]]
              (let [a (parse-address (:listen-address v))]
                (assoc m
                       n
                       (or (get backends n)
                           (assoc v
                                  :name n
                                  :chan
                                  (backend-chan (:address a) (:port a) listener)
                                  :last-seen-at-ms
                                  (if-let [ls (:last-seen-at v)]
                                    (s->millepoch-time ls)
                                    0)
                                  )))))
            {}
            (:backends config))))

(defn get-port [listener]
  (.. listener socket getLocalPort))

(defn update-listener [listener config]
  (if-let [req-addr (get-in config [:config :listen-address])]
    (let [req-port (:port (parse-address req-addr))
          old-port (and listener (get-port listener))]
      (if (and listener
               (= old-port req-port))
        listener
        (let [new-l (ServerSocketChannel/open)]
          (and listener (.close listener))
          (doto new-l
            (.configureBlocking false)
            (.bind (InetSocketAddress. req-port))))))
    nil))

(defn combined-config-watcher [prefix]
  (let [config-watcher (etcd/watch-prefix prefix)
        out-ch (chan)]
    (go
      (loop [backend-prefix nil
             backend-watcher nil]
        (let [config (etcd/get-prefix prefix)
              new-bp (:upstream-service-etcd-prefix config)
              backend-watcher
              (if (and backend-watcher (= new-bp backend-prefix))
                backend-watcher
                (and new-bp (etcd/watch-prefix new-bp)))]
          (>! out-ch {:config config
                      :backends (if new-bp (etcd/get-prefix new-bp) {})})
          (alts! (remove nil? [config-watcher backend-watcher]))
          (recur new-bp backend-watcher))))
    out-ch))

(defn seen-since? [timestamp backend]
  (> (:last-seen-at-ms backend) timestamp))

(defn healthy-backends [timestamp backends]
  (let [h (filter (partial seen-since? timestamp) (vals backends))]
    (if (seq h)
      (println ["backends " (map :name h)])
      (println ["no healthy backends "]))
    h))

(defn run-server [prefix]
  (let [config-ch (combined-config-watcher prefix)
        shutdown (chan)]
    (go
      (loop [backends {}
             listener nil
             config {}]
        (let [timestamp (- (millepoch-time-now) (* 3600 1000))
              healthy (healthy-backends timestamp backends)
              [val ch] (alts! [config-ch shutdown])]
          (cond (= ch config-ch)
                (let [l (update-listener listener val)]
                  (recur (update-backends backends val l)
                         l
                         val))

                (= ch shutdown)
                (do (println "quit") (.close listener))

                :else
                (recur backends listener config)))))
    shutdown))
