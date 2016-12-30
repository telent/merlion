(ns merlion.core
  (:import [java.nio Buffer ByteBuffer]
           [java.net InetSocketAddress]
           [java.nio.channels SocketChannel ServerSocketChannel
            Selector SelectionKey])
  (:require [merlion.etcd :as etcd]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :as async
             :refer [<! <!! go chan >! close! alts!]]))

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

(defn relay-to-backend [socketchannel host port]
  (.configureBlocking socketchannel false)
  (let [sbuffer (ByteBuffer/allocate 8192)
        obuffer (ByteBuffer/allocate 8192)
        bo (Selector/open)
        skey (.register socketchannel bo SelectionKey/OP_READ)
        outchannel (doto (SocketChannel/open)
                     (.connect (InetSocketAddress. host port))
                     (.configureBlocking false))
        skey2 (.register outchannel bo SelectionKey/OP_READ)]
    (loop []
      (let [ready (.select bo)
            keys (.selectedKeys bo)
            key (first (seq keys))]
        (when key
          (.remove keys key)
          (condp = (.channel key)
            socketchannel
            (when (copy-channel socketchannel outchannel sbuffer) (recur))
            outchannel
            (when (copy-channel outchannel socketchannel obuffer) (recur))))))
    (.close outchannel)
    (.close bo)
    ))

(defn backend-chan
  "a channel that repeatedly receives a nio ServerSocketChannel, accepts a new socket from it and relays the data being sent from/to it onto a new connection to host:port"
  [host port]
  (let [ch (chan)]
    (future
      (loop []
        (let [ssc (<!! ch)]
          (when ssc
            (let [socketchannel (.accept ssc)]
              (relay-to-backend socketchannel host port)
              (.close socketchannel))
            (recur))))
      (println ["backend thread quit" host port]))
    ch))

(defn parse-address [a]
  (let [[address port] (str/split a #":")]
    {:address address :port (Integer/parseInt port)}))

(defn s->millepoch-time [s]
  (.getTimeInMillis (clojure.instant/read-instant-calendar s)))

(defn millepoch-time-now []
  (.getTime (java.util.Date.)))

(defn update-backends [backends config]
  (let [existing (keys backends)
        wanted (keys (:backends config))
        unwanted (clojure.set/difference (set existing) (set wanted))]
    (run! close! (map #(:chan (get backends %)) unwanted))
    (reduce (fn [m [n v]]
              (let [a (parse-address (:listen-address v))]
                (assoc m
                       n
                       (or ;(get backends n)
                           (assoc v
                                  :name n
                                  :chan (backend-chan (:address a) (:port a))
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
          (doto new-l (.bind (InetSocketAddress. req-port))))))
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
  (let [config-ch (merlion.core/combined-config-watcher prefix)
        shutdown (chan)]
    (go
      (loop [backends {}
             listener nil
             config {}]
        (let [timestamp (- (millepoch-time-now) (* 10 1000))
              healthy (healthy-backends timestamp backends)
              [val ch] (alts!
                        (conj
                         (map #(vector (:chan %) listener)
                              healthy)
                         config-ch
                         shutdown))]
          (cond (= ch config-ch)
                (recur (update-backends backends val)
                       (update-listener listener val)
                       val)

                (= ch shutdown)
                (do (println "quit") (.close listener))

                :else
                (recur backends listener config)))))
    shutdown))
