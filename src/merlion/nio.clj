(ns merlion.nio
  (:import [java.nio Buffer ByteBuffer]
           [java.net InetSocketAddress]
           [java.nio.channels SocketChannel ServerSocketChannel
            Selector SelectionKey])
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.core.async :as async
             :refer [<! go chan >! close! alts!]]))

(defn server-socket [port backends]
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
    (go
      (loop []
        (let [socketchannel (<! ch)]
          (when socketchannel
            (relay-to-backend socketchannel host port)
            (.close socketchannel)
            (recur)))))
    ch))

(defn bend [port]
  (let [backend (backend-chan "127.0.0.1" 8023)]
    (server-socket port [backend])
    backend))
