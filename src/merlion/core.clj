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
  "a channel that accepts nio SocketChannels and relays the data being sent to them onto a new connection to host:port"
  [host port]
  (let [ch (chan)]
    (future
      (loop []
        (let [socketchannel (.accept (<!! ch))]
          (when socketchannel
            (when (instance? SocketChannel socketchannel)
              (relay-to-backend socketchannel host port)
              (.close socketchannel))
            (recur)))))
    ch))

(defn parse-address [a]
  (let [[address port] (str/split a #":")]
    {:address address :port (Integer/parseInt port)}))

(defn update-backends [backends config]
  (pprint [:in-update-backends config])
  (map #(let [a (parse-address (:listen-address %))]
          (assoc % :chan (backend-chan (:address a) (:port a))))
       (vals (:backends config))))

(defn get-port [listener]
  (.. listener socket getLocalPort))

(defn update-listener [listener config]
  (if-let [req-addr (get-in config [:config :listen-address])]
    (let [req-port (:port (parse-address req-addr))]
      (if (and listener
               (= (get-port listener) req-port))
        listener
        (doto (ServerSocketChannel/open) (.bind (InetSocketAddress. req-port)))))
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

(defn run-server [prefix]
  ;; what are we trying to do?
  ;; listen for config changes
  ;; - when new backends, create backend-chan for them
  ;;   (how do we update existing listeners to see new backends?)
  ;; - when new listen-address, open a serversocketchannel
  ;; - when any backend ready for a new connection, send it the latest
  ;;   result of accept or something it can use to call accept
  (let [config-ch (merlion.core/combined-config-watcher prefix)
        shutdown (chan)]
    (go
      (loop [backends []
             listener nil
             config {}]
        (let [[val ch] (alts!
                        (conj
                         (map #(vector (:chan %) listener) backends)
                         config-ch
                         shutdown))]
          (cond (= ch config-ch)
                (recur (update-backends backends val)
                       (update-listener listener val)
                       val)

                (= ch shutdown)
                (do (println "quit") (.close listener))

                :else
                (do (and listener (>! ch listener))
                    (recur backends listener config))))))
    shutdown))
