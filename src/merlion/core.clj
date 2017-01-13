(ns merlion.core
  (:import [java.nio Buffer ByteBuffer]
           [java.net InetSocketAddress]
           [java.nio.channels SocketChannel ServerSocketChannel
            Selector SelectionKey])
  (:gen-class)
  (:require [merlion.etcd :as etcd]
            [clojure.string :as str]
            [clojure.test :as test :refer [deftest testing is]]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [taoensso.timbre :as log :refer [debug info]]
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


(defn write-pending-buffer [w]
  (let [ch (:to-channel w)
        b (:buffer w)]
    (if (:eof? w)
      w
      (do
        (.write ch b)
        (if (.hasRemaining b)
          w
          (do (.clear b) nil))))))

(defn read-pending-buffer [buffer from-channel to-channel]
  (let [b buffer
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

(defn poll-selector [bo timeout]
  (if-not (zero? (.select bo timeout))
    (let [ready (.selectedKeys bo)
          v (doall (map (fn [k]
                          {:channel (.channel k)
                           :ops (.readyOps k)})
                        ready))]
      (.clear ready)
      v)))

(defn poll-channels [poll-set timeout]
  (with-open [bo (reduce (fn [s [channel & ops]]
                           (.register channel s (poll-ops-mask ops))
                           s)
                         (Selector/open)
                         poll-set)]
    (doall ; have to force evaluation of this before closing selector
     (poll-selector bo timeout))))

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
        (is (ready-for? (poll-channels [[ssh :read]] 1357) ssh :read))))))

(defn accept-connection [socket]
  (let [s1 (.accept socket)]
    (when s1
      (when (log/may-log? :debug)
        ;; no need to getRemoteAddress if we don't have debug logging
        (log/debug (str "accepted connection from " (.getRemoteAddress s1))))
      (doto s1
        (.configureBlocking false)))))

(defn new-selector [channel ops]
  (let [s (Selector/open)]
    (.register channel s (poll-ops-mask ops))
    s))

(defn update-selector-for-writing [selector w]
  (when-not (:eof? w)
    (let [c (:to-channel w)]
      (run! #(or (= c (.channel %)) (.cancel %)) (.keys selector))
      (.register c selector (poll-ops-mask [:write]))
    selector)))


(defn backend [serversocket host port finished-ch]
  (let [bytebuffer (ByteBuffer/allocate 8192)
        selector (new-selector serversocket [:accept])
        write-selector (Selector/open)]
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
            sel (or
                 (and pending-write
                      (update-selector-for-writing write-selector pending-write))
                 selector)]
        (let [ready (poll-selector sel 10000)
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
            ;; when we get a finish message we take the serversocket out of
            ;; our selector, so we can finish up serving the requests we already
            ;; started but will not pick up any new connections
            (async/poll! finished-ch)
            (let [key (first (filter #(= serversocket (.channel %))
                                     (.keys selector)))]
              ;; if the serversocket has been closed already (e.g. by another
              ;; thread when the listen-address is changed) then it will not
              ;; appear in this selector
              (if key (.cancel key))
              (recur pending-write downstreams-for-upstreams))

            ;; if we have no serversocket and no open channels, it's
            ;; byebye time
            (empty? (.keys selector))
            nil

            ;; Zeroth, close the corresponding down/upstream for any up/downstream
            ;; on which we received eof while reading
            (and pending-write (:eof? pending-write))
            (let [fch (:from-channel pending-write)
                  tch (:to-channel pending-write)]
              (.close fch)              ;this also causes removal of these
              (.close tch)              ;channels from the selector
              (recur nil (dissoc downstreams-for-upstreams fch tch)))

            ;; First deal with any unwritten buffer to/from the
            ;; downstream.  We do not
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
                (recur (write-pending-buffer
                        (read-pending-buffer bytebuffer
                                             c (upstreams-for-downstreams c)))
                       downstreams-for-upstreams)))

            ;; Next look at new work from existing upstream requests
            readable-us
            (do
              (let [c (:channel readable-us)]
                (recur (write-pending-buffer
                        (read-pending-buffer bytebuffer c
                                             (downstreams-for-upstreams c)))
                       downstreams-for-upstreams)))

            ;; We can pick up a new upstream request if nothing else to do.
            ;; Note this may block in connect(), I haven't decided if that's
            ;; bad or not
            (get ready-channels serversocket)
            (if-let [sock (accept-connection serversocket)]
              (let [ds (doto (SocketChannel/open)
                         (.connect (InetSocketAddress. host port))
                         (.configureBlocking false))
                    mask (poll-ops-mask [:read])]
                (.register sock selector mask)
                (.register ds selector mask)
                (recur nil (assoc downstreams-for-upstreams sock ds)))
              (recur nil downstreams-for-upstreams))

            :default
            (recur pending-write downstreams-for-upstreams)
            ))))))



(defn backend-chan
  [host port listener]
  (info (str "started backend for " host ":" port))
  (let [ch (chan)]
    (future
      (backend listener host port ch)
      (info (str "backend " host  ":" port " quit")))
    ch))

(defn parse-address [a]
  (let [[address port] (str/split a #":")]
    {:address address :port (Integer/parseInt port)}))

(defn update-backends [backends config listener]
  (let [existing (keys backends)
        wanted (map #(vector % listener) (keys (:backends config)))
        unwanted (set/difference (set existing) (set wanted))]
    (log/spy :trace unwanted)
    (run! #(async/put! % :quit) (map #(:chan (get backends %)) unwanted))
    (reduce (fn [m [n v]]
              (let [a (parse-address (:listen-address v))]
                (assoc m
                       [n listener]
                       (or (get backends [n listener])
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
  (when-let [l (keyword (get-in config [:config :log-level]))]
    (when-not (= l (:level log/*config*))
      (log/info (str "Setting log level to " l))
      (log/set-level! (keyword l))))
  (if-let [req-addr (log/spy :trace (get-in config [:config :listen-address]))]
    (let [req-port (:port (parse-address req-addr))
          old-port (and listener (get-port listener))]
      (if (and listener
               (= old-port req-port))
        (log/spy :trace listener)
        (let [new-l (ServerSocketChannel/open)]
          (and listener (.close listener))
          (doto (log/spy :trace new-l)
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
    (log/spy :trace h)))

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
                (do (info "server quit") (.close listener))

                :else
                (recur backends listener config)))))
    shutdown))

(defn -main [prefix]
  (log/set-level! :debug)
  (run-server prefix)
  (while true (Thread/sleep 5000)))
