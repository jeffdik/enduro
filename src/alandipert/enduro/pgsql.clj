(ns alandipert.enduro.pgsql
  (:require [alandipert.enduro :as e]
            [clojure.java.jdbc :as sql]))

(defn create-enduro-table! [db table-name value]
  (sql/with-db-transaction [t-con db]
    (sql/execute! t-con (sql/create-table-ddl table-name [[:id :int] [:value :text]]))
    (sql/insert! t-con table-name {:id 0 :value (pr-str value)})))

(defn delete-enduro-table! [db table-name]
  (sql/db-do-commands db (sql/drop-table-ddl table-name)))

(defn table-exists? [db table-name]
  (->> (sql/query db ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"])
       (some #(= table-name (:table_name %)))))

(defn get-value [db table-name]
  (->> (sql/query db [(str "SELECT value FROM " table-name " LIMIT 1")])
       first
       :value
       read-string))

(deftype PostgreSQLBackend [db-config table-name]
  e/IDurableBackend
  (-commit!
    [this value]
    (sql/update! db-config table-name {:value (pr-str value)} ["id = ?" 0]))
  (-remove! [this]
    (delete-enduro-table! db-config table-name)))

(defn postgresql-atom
  #=(e/with-options-doc "Creates and returns a PostgreSQL-backed atom. If the location
  denoted by the combination of db-config and table-name exists, it is
  read and becomes the initial value. Otherwise, the initial value is
  init and the table denoted by table-name is updated.

  db-config can be a String URI, a map
  of :username/:password/:host/:port, or any other type of
  configuration object understood by
  clojure.java.jdbc/with-connection")
  [init db-config table-name & opts]
  (e/atom*
   (sql/with-db-connection [conn db-config]
     (or (and (table-exists? conn table-name)
              (get-value conn table-name))
         (do
           (create-enduro-table! conn table-name init)
           init)))
   (PostgreSQLBackend. db-config table-name)
   (apply hash-map opts)))
