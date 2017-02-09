(ns merlion.core
  (:import [java.nio Buffer ByteBuffer]
           [java.net InetSocketAddress]
           [java.nio.channels SocketChannel ServerSocketChannel
            Selector SelectionKey])
  (:gen-class)
  (:require [merlion.etcd :as etcd]
            [merlion.config :as conf]
            [clojure.string :as str]
            [clojure.test :as test :refer [deftest testing is]]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec :as s]
            [taoensso.timbre :as log :refer [debug info]]
            [clojure.core.async :as async
             :refer [<! <!! go chan >! close! alts!]]))

(defn millepoch-time-now []
  (.getTime (java.util.Date.)))

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

(defn backend [serversocket address finished-ch on-exit]
  (let [bytebuffer (ByteBuffer/allocate 8192)
        {:keys [::conf/host ::conf/port]} address
        selector (new-selector serversocket [:accept])
        write-selector (Selector/open)]
    (info (str "started backend for " address))
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
                             ready))]

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
            (if-let [sock (log/spy :trace (accept-connection serversocket))]
              (let [ds (try
                         (doto (SocketChannel/open)
                           (.connect (InetSocketAddress. host port))
                           (debug "connected")
                           (.configureBlocking false))
                         (catch java.net.ConnectException e
                           (let [m (str "Cannot connect to " host ":" port)]
                             (log/warn m)
                             (.close sock)
                             (on-exit m)
                             nil)))
                    mask (poll-ops-mask [:read])]
                (when ds
                  (.register sock selector mask)
                  (.register ds selector mask)
                  (recur nil (assoc downstreams-for-upstreams sock ds))))
              (recur nil downstreams-for-upstreams))

            :default
            (recur pending-write downstreams-for-upstreams)))))))

(defn backend-chan
  [address listener on-exit]
  (let [ch (chan)]
    (future
      (backend listener address ch on-exit))
    ch))

;; a few simple rules
;; there should be backend state for every backend named in the spec
;; it may be disabled because
;; - disabled in spec
;; - no recent last-seen
;; - last-seen before last network fail
;; if it is disabled then we should stop its thread and forget its chan
;; if it is not disabled and it has no chan, make one

(s/def ::exited-at ::conf/timestamp)

;; (s/def ::chan async/chan?) no such fn

(s/def ::backend-state
  (s/keys :req-un [::conf/last-seen-at
                   ::conf/listen-address]
          :opt-un [::exited-at
                   ::exit-reason
                   ::chan
                   ]))

(s/conform ::backend-state
           {:last-seen-at "2017-01-26T16:42:46+00:00"
            :listen-address "etwert:1000"
            :chan true})
(s/conform ::conf/last-seen-at nil)

(defn backend-unavailable? [margin backend-state]
  (or (empty? backend-state)
      (if-let [last-seen (:last-seen-at (log/spy backend-state))]
        (or (if-let [ex (:exited-at backend-state)] (> ex last-seen))
            (> (- (millepoch-time-now) last-seen) (* 1000 margin)))
        true)))

(defn update-backend-state-from-spec [margin exit-chan listener backend spec]
  {:pre [(s/valid? ::conf/backend spec)]
   :post [(s/valid? ::backend-state %)]}
  (let [last-seen (or (:last-seen-at spec) 0)
        be (assoc backend
                  :last-seen-at last-seen
                  :listen-address (:listen-address spec))
        unavailable? (log/spy (backend-unavailable? margin (log/spy be)))
        disabled? (or (:disabled spec) unavailable?)]
    (if-let [c (and disabled? (:chan backend))]
      (async/put! c :quit))
    (assoc be
           :unavailable unavailable?
           :chan (if disabled?
                   nil
                   (or (:chan backend)
                       (backend-chan
                        (:listen-address spec)
                        listener
                        #(async/put! exit-chan [[(:name spec) listener] %])))))))

(defn update-backends [backends config listener exit-chan]
  (log/spy config)
  (let [existing (keys backends)
        wanted (map #(vector % listener) (keys (:backends config)))
        margin (:upstream-freshness (:config config))
        unwanted (set/difference (set existing) (set wanted))]
    (log/spy :trace unwanted)
    (run! #(async/put! % :quit) (map #(:chan (get backends %)) unwanted))
    (reduce (fn [m [n spec]]
              (let [state (get backends [n listener] {})]
                (assoc
                 m
                 [n listener]
                 (update-backend-state-from-spec margin exit-chan
                                                 listener state
                                                 (assoc spec :name n)))))
            {}
            (:backends config))))

(defn get-port [listener]
  (.. listener socket getLocalPort))

(defn update-listener [listener config]
  (when-let [l (get-in config [:config :log-level])]
    (when-not (= l (:level log/*config*))
      (log/info (str "Setting log level to " l))
      (log/set-level! l)))
  (if-let [req-addr (log/spy :trace (get-in config [:config :listen-address]))]
    (let [req-port (::conf/port req-addr)
          old-port (and listener (get-port listener))]
      (if (and listener
               (= old-port req-port))
        (log/spy :trace listener)
        (let [new-l (ServerSocketChannel/open)]
          (and listener (.close listener))
          (doto  new-l
            (.configureBlocking false)
            (.bind (InetSocketAddress. req-port))))))
    nil))

(defn conform-or-warn [spec data]
  (if (s/valid? spec data)
    (s/conform spec data)
    (log/warn (pr-str (s/explain-data spec data)))))

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
          (>! out-ch
              {:config (conform-or-warn ::conf/config (conf/with-defaults config))
               :backends
               (if new-bp
                 (conform-or-warn
                  (s/map-of keyword? ::conf/backend)
                  (etcd/get-prefix new-bp))
                 {})})
          (alts! (remove nil? [config-watcher backend-watcher]))
          (recur new-bp backend-watcher))))
    out-ch))

(defn public-backend-state [backends]
  (reduce (fn [m [[n l] v]]
            (assoc m
                   n
                   (select-keys v [:last-seen-at
                                   :disabled
                                   :unavailable
                                   :exit-reason
                                   :exited-at])))
          {}
          backends))

(defn most-stale-backend-timestamp [backends]
  (reduce min (millepoch-time-now)
          (map :last-seen-at
               (filter (complement :disabled) (vals backends)))))

(defn time-to-next-expire [config backend-states]
  (and (not (empty? config)) (not (empty? backend-states))
       (let [freshness (* 1000 (:upstream-freshness (:config config)))
             interval
             (- (+ (most-stale-backend-timestamp backend-states) freshness)
                (millepoch-time-now))]
         (if (> interval 0) interval nil))))

(defn run-server [prefix quit-chan]
  (let [config-ch (combined-config-watcher prefix)
        backend-exits (chan)
        shutdown quit-chan]
    (go
      (loop [backends {}
             listener nil
             config {}]
        (if-let [state-prefix (:state-etcd-prefix (:config config))]
          (if-not (empty? backends)
            (etcd/put-map (str state-prefix "/backends")
                          (log/spy (public-backend-state (log/spy backends))))))
        (let [next-check-interval (log/spy (time-to-next-expire config backends))
              timeout-ch (async/timeout (or next-check-interval (* 1000 1000)))
              [val ch] (alts! [config-ch shutdown backend-exits timeout-ch])]
          (cond (= ch config-ch)
                (let [l (update-listener listener val)]
                  (log/debug (pr-str "new config " val))
                  (if (:config val)
                    (recur (update-backends backends val l backend-exits)
                           l
                           val)
                    (log/fatal
                     (str "no valid configuration found at etcd prefix "
                          (pr-str prefix)))
                    ))

                (= ch backend-exits)
                (let [[backend-key reason] val]
                  (log/info (str "backend " (first backend-key) " quit: " reason))
                  (recur (update-in backends [backend-key]
                                    (fn [b]
                                      (assoc b :exited-at (millepoch-time-now)
                                             :exit-reason reason)))
                         listener
                         config))

                (= ch timeout-ch)
                (do
                  (log/trace "expire")
                  (recur (update-backends backends config listener backend-exits)
                         listener
                         config))

                (= ch shutdown)
                (do (info "server quit") (.close listener))

                :else
                (recur backends listener config))))
      true)))

(defn -main [prefix]
  (log/set-level! :debug)
  (let [c (chan)]
    ;; sending stuff to c will cause exit.  ideally we would have a
    ;; signal handler or something that does that
    (<!! (run-server prefix c))))
