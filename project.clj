(defproject com.verybigthings/penkala "0.1.0"
  :description "Composable query builder for PostgreSQL."
  :url "https://github.com/retro/penkala"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :source-paths ["src"]
  :dependencies [[com.layerware/hugsql "0.5.1"]
                 [com.layerware/hugsql-adapter-next-jdbc "0.5.1"]
                 [seancorfield/next.jdbc "1.1.610"]
                 [org.postgresql/postgresql "42.2.18"]
                 [camel-snake-kebab/camel-snake-kebab "0.4.2"]
                 [com.github.vertical-blank/sql-formatter "1.0.3"]
                 [metosin/jsonista "0.2.7"]])