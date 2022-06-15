(defproject com.verybigthings/penkala "0.9.1"
  :description "Composable query builder for PostgreSQL."
  :url "https://github.com/retro/penkala"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :source-paths ["src"]
  :dependencies [[com.layerware/hugsql "0.5.3"]
                 [com.layerware/hugsql-adapter-next-jdbc "0.5.3"]
                 [com.github.seancorfield/next.jdbc "1.2.780"]
                 [org.postgresql/postgresql "42.3.6"]
                 [camel-snake-kebab/camel-snake-kebab "0.4.2"]
                 [com.github.vertical-blank/sql-formatter "2.0.3"]
                 [metosin/jsonista "0.2.7"]
                 [com.stuartsierra/dependency "1.0.0"]])
