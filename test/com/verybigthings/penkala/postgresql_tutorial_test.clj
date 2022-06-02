(ns com.verybigthings.penkala.postgresql-tutorial-test
  "This namespace implements test cases based on the queries found on
   https://www.postgresqltutorial.com/. You can follow the tutorials
   and see how are queries implemented with Penkala"
  (:require [clojure.test :refer [deftest testing use-fixtures]]
            [com.verybigthings.penkala.next-jdbc :refer [insert! select!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]
            [testit.core :refer [fact facts =in=>]]))

;;(use-fixtures :each (partial th/reset-db-fixture "pagila"))
(use-fixtures :once th/pagila-db-fixture)

(defn date? [val]
  (= java.time.LocalDate (class val)))

(defn datetime? [val]
  (= java.time.LocalDateTime (class val)))

(deftest select
  ;; https://www.postgresqltutorial.com/postgresql-select/
  (testing "Query data from one column"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:first-name "MARY"}
                  #:customer{:first-name "PATRICIA"}
                  #:customer{:first-name "LINDA"}
                  #:customer{:first-name "BARBARA"}
                  #:customer{:first-name "ELIZABETH"}
                  #:customer{:first-name "JENNIFER"}
                  #:customer{:first-name "MARIA"}
                  #:customer{:first-name "SUSAN"}
                  #:customer{:first-name "MARGARET"}
                  #:customer{:first-name "DOROTHY"}])))

  (testing "Query data from multiple columns"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name :email]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:email "MARY.SMITH@sakilacustomer.org"
                             :last-name "SMITH"
                             :first-name "MARY"}
                  #:customer{:email "PATRICIA.JOHNSON@sakilacustomer.org"
                             :last-name "JOHNSON"
                             :first-name "PATRICIA"}
                  #:customer{:email "LINDA.WILLIAMS@sakilacustomer.org"
                             :last-name "WILLIAMS"
                             :first-name "LINDA"}
                  #:customer{:email "BARBARA.JONES@sakilacustomer.org"
                             :last-name "JONES"
                             :first-name "BARBARA"}
                  #:customer{:email "ELIZABETH.BROWN@sakilacustomer.org"
                             :last-name "BROWN"
                             :first-name "ELIZABETH"}
                  #:customer{:email "JENNIFER.DAVIS@sakilacustomer.org"
                             :last-name "DAVIS"
                             :first-name "JENNIFER"}
                  #:customer{:email "MARIA.MILLER@sakilacustomer.org"
                             :last-name "MILLER"
                             :first-name "MARIA"}
                  #:customer{:email "SUSAN.WILSON@sakilacustomer.org"
                             :last-name "WILSON"
                             :first-name "SUSAN"}
                  #:customer{:email "MARGARET.MOORE@sakilacustomer.org"
                             :last-name "MOORE"
                             :first-name "MARGARET"}
                  #:customer{:email "DOROTHY.TAYLOR@sakilacustomer.org"
                             :last-name "TAYLOR"
                             :first-name "DOROTHY"}])))

  (testing "Query data from all columns"
    (let [customer (-> *env*
                       :customer)
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:active 1
                             :activebool true
                             :address-id 5
                             :create-date date?
                             :customer-id 1
                             :email "MARY.SMITH@sakilacustomer.org"
                             :first-name "MARY"
                             :last-name "SMITH"
                             :last-update datetime?
                             :store-id 1}
                  #:customer{:active 1
                             :activebool true
                             :address-id 6
                             :create-date date?
                             :customer-id 2
                             :email "PATRICIA.JOHNSON@sakilacustomer.org"
                             :first-name "PATRICIA"
                             :last-name "JOHNSON"
                             :last-update datetime?
                             :store-id 1}
                  #:customer{:active 1
                             :activebool true
                             :address-id 7
                             :create-date date?
                             :customer-id 3
                             :email "LINDA.WILLIAMS@sakilacustomer.org"
                             :first-name "LINDA"
                             :last-name "WILLIAMS"
                             :last-update datetime?
                             :store-id 1}
                  #:customer{:active 1
                             :activebool true
                             :address-id 8
                             :create-date date?
                             :customer-id 4
                             :email "BARBARA.JONES@sakilacustomer.org"
                             :first-name "BARBARA"
                             :last-name "JONES"
                             :last-update datetime?
                             :store-id 2}
                  #:customer{:active 1
                             :activebool true
                             :address-id 9
                             :create-date date?
                             :customer-id 5
                             :email "ELIZABETH.BROWN@sakilacustomer.org"
                             :first-name "ELIZABETH"
                             :last-name "BROWN"
                             :last-update datetime?
                             :store-id 1}
                  #:customer{:active 1
                             :activebool true
                             :address-id 10
                             :create-date date?
                             :customer-id 6
                             :email "JENNIFER.DAVIS@sakilacustomer.org"
                             :first-name "JENNIFER"
                             :last-name "DAVIS"
                             :last-update datetime?
                             :store-id 2}
                  #:customer{:active 1
                             :activebool true
                             :address-id 11
                             :create-date date?
                             :customer-id 7
                             :email "MARIA.MILLER@sakilacustomer.org"
                             :first-name "MARIA"
                             :last-name "MILLER"
                             :last-update datetime?
                             :store-id 1}
                  #:customer{:active 1
                             :activebool true
                             :address-id 12
                             :create-date date?
                             :customer-id 8
                             :email "SUSAN.WILSON@sakilacustomer.org"
                             :first-name "SUSAN"
                             :last-name "WILSON"
                             :last-update datetime?
                             :store-id 2}
                  #:customer{:active 1
                             :activebool true
                             :address-id 13
                             :create-date date?
                             :customer-id 9
                             :email "MARGARET.MOORE@sakilacustomer.org"
                             :first-name "MARGARET"
                             :last-name "MOORE"
                             :last-update datetime?
                             :store-id 2}
                  #:customer{:active 1
                             :activebool true
                             :address-id 14
                             :create-date date?
                             :customer-id 10
                             :email "DOROTHY.TAYLOR@sakilacustomer.org"
                             :first-name "DOROTHY"
                             :last-name "TAYLOR"
                             :last-update datetime?
                             :store-id 1}])))

  (testing "Expressions example"
    (let [customer (-> *env*
                       :customer
                       (r/extend :full-name [:concat :first-name " " :last-name])
                       (r/select [:full-name :email]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]

      (fact
       res =in=> [#:customer{:email "MARY.SMITH@sakilacustomer.org"
                             :full-name "MARY SMITH"}
                  #:customer{:email "PATRICIA.JOHNSON@sakilacustomer.org"
                             :full-name "PATRICIA JOHNSON"}
                  #:customer{:email "LINDA.WILLIAMS@sakilacustomer.org"
                             :full-name "LINDA WILLIAMS"}
                  #:customer{:email "BARBARA.JONES@sakilacustomer.org"
                             :full-name "BARBARA JONES"}
                  #:customer{:email "ELIZABETH.BROWN@sakilacustomer.org"
                             :full-name "ELIZABETH BROWN"}
                  #:customer{:email "JENNIFER.DAVIS@sakilacustomer.org"
                             :full-name "JENNIFER DAVIS"}
                  #:customer{:email "MARIA.MILLER@sakilacustomer.org"
                             :full-name "MARIA MILLER"}
                  #:customer{:email "SUSAN.WILSON@sakilacustomer.org"
                             :full-name "SUSAN WILSON"}
                  #:customer{:email "MARGARET.MOORE@sakilacustomer.org"
                             :full-name "MARGARET MOORE"}
                  #:customer{:email "DOROTHY.TAYLOR@sakilacustomer.org"
                             :full-name "DOROTHY TAYLOR"}]))))

(deftest column-alias
  ;; https://www.postgresqltutorial.com/postgresql-column-alias/
  (testing "Assigning a column alias"
    (let [customer (-> *env*
                       :customer
                       (r/rename :last-name :surname)
                       (r/select [:surname]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:surname "SMITH"}
                  #:customer{:surname "JOHNSON"}
                  #:customer{:surname "WILLIAMS"}
                  #:customer{:surname "JONES"}
                  #:customer{:surname "BROWN"}
                  #:customer{:surname "DAVIS"}
                  #:customer{:surname "MILLER"}
                  #:customer{:surname "WILSON"}
                  #:customer{:surname "MOORE"}
                  #:customer{:surname "TAYLOR"}]))))

(deftest order-by
  ;; https://www.postgresqltutorial.com/postgresql-order-by/
  (testing "Order by one column ASC"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/order-by [[:first-name :asc]]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "SELBY"
                             :first-name "AARON"}
                  #:customer{:last-name "GOOCH"
                             :first-name "ADAM"}
                  #:customer{:last-name "CLARY"
                             :first-name "ADRIAN"}
                  #:customer{:last-name "BISHOP"
                             :first-name "AGNES"}
                  #:customer{:last-name "KAHN"
                             :first-name "ALAN"}
                  #:customer{:last-name "CROUSE"
                             :first-name "ALBERT"}
                  #:customer{:last-name "HENNING"
                             :first-name "ALBERTO"}
                  #:customer{:last-name "GRESHAM"
                             :first-name "ALEX"}
                  #:customer{:last-name "FENNELL"
                             :first-name "ALEXANDER"}
                  #:customer{:last-name "CASILLAS"
                             :first-name "ALFRED"}])))

  (testing "Order by one column omitting ASC default"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/order-by [:first-name]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "SELBY"
                             :first-name "AARON"}
                  #:customer{:last-name "GOOCH"
                             :first-name "ADAM"}
                  #:customer{:last-name "CLARY"
                             :first-name "ADRIAN"}
                  #:customer{:last-name "BISHOP"
                             :first-name "AGNES"}
                  #:customer{:last-name "KAHN"
                             :first-name "ALAN"}
                  #:customer{:last-name "CROUSE"
                             :first-name "ALBERT"}
                  #:customer{:last-name "HENNING"
                             :first-name "ALBERTO"}
                  #:customer{:last-name "GRESHAM"
                             :first-name "ALEX"}
                  #:customer{:last-name "FENNELL"
                             :first-name "ALEXANDER"}
                  #:customer{:last-name "CASILLAS"
                             :first-name "ALFRED"}])))

  (testing "Order by one column DESC"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/order-by [[:first-name :desc]]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "HITE"
                             :first-name "ZACHARY"}
                  #:customer{:last-name "WATKINS"
                             :first-name "YVONNE"}
                  #:customer{:last-name "WEAVER"
                             :first-name "YOLANDA"}
                  #:customer{:last-name "RICHARDS"
                             :first-name "WILMA"}
                  #:customer{:last-name "MARKHAM"
                             :first-name "WILLIE"}
                  #:customer{:last-name "HOWELL"
                             :first-name "WILLIE"}
                  #:customer{:last-name "SATTERFIELD"
                             :first-name "WILLIAM"}
                  #:customer{:last-name "LUMPKIN"
                             :first-name "WILLARD"}
                  #:customer{:last-name "BULL"
                             :first-name "WESLEY"}
                  #:customer{:last-name "HARRISON"
                             :first-name "WENDY"}])))

  (testing "Order by multiple columns"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/order-by [[:first-name :asc] [:last-name :desc]]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:last-name "SELBY"
                             :first-name "AARON"}
                  #:customer{:last-name "GOOCH"
                             :first-name "ADAM"}
                  #:customer{:last-name "CLARY"
                             :first-name "ADRIAN"}
                  #:customer{:last-name "BISHOP"
                             :first-name "AGNES"}
                  #:customer{:last-name "KAHN"
                             :first-name "ALAN"}
                  #:customer{:last-name "CROUSE"
                             :first-name "ALBERT"}
                  #:customer{:last-name "HENNING"
                             :first-name "ALBERTO"}
                  #:customer{:last-name "GRESHAM"
                             :first-name "ALEX"}
                  #:customer{:last-name "FENNELL"
                             :first-name "ALEXANDER"}
                  #:customer{:last-name "CASILLAS"
                             :first-name "ALFRED"}])))

  (testing "Order by expression"
    (let [customer (-> *env*
                       :customer
                       (r/extend :len [:length :first-name])
                       (r/select [:first-name :len])
                       (r/order-by [[:len :desc]]))
          res (-> (select! *env* customer)
                  (subvec 0 10))]
      (fact
       res =in=> [#:customer{:first-name "CHRISTOPHER"
                             :len 11}
                  #:customer{:first-name "JACQUELINE"
                             :len 10}
                  #:customer{:first-name "FREDERICK"
                             :len 9}
                  #:customer{:first-name "CASSANDRA"
                             :len 9}
                  #:customer{:first-name "CHRISTINE"
                             :len 9}
                  #:customer{:first-name "CHRISTINA"
                             :len 9}
                  #:customer{:first-name "JOSEPHINE"
                             :len 9}
                  #:customer{:first-name "GWENDOLYN"
                             :len 9}
                  #:customer{:first-name "CATHERINE"
                             :len 9}
                  #:customer{:first-name "CONSTANCE"
                             :len 9}])))

  (testing "Order by and null"
    (let [sort-demo (-> *env*
                        :sort-demo
                        (r/select [:num])
                        (r/order-by [:num]))
          ;; We pass {:keep-nil? true} as the decomposition schema
          ;; override because otherwise Penkala will remove rows
          ;; where all primary key values are nil
          res (select! *env* sort-demo {} {:keep-nil? true})]
      (fact
       res =in=> [#:sort-demo{:num 1}
                  #:sort-demo{:num 2}
                  #:sort-demo{:num 3}
                  #:sort-demo{:num nil}]))

    (testing "Order by and NULLS FIRST"
      (let [sort-demo (-> *env*
                          :sort-demo
                          (r/select [:num])
                          (r/order-by [[:num :asc :nulls-first]]))
            ;; We pass {:keep-nil? true} as the decomposition schema
            ;; override because otherwise Penkala will remove rows
            ;; where all primary key values are nil
            res (select! *env* sort-demo {} {:keep-nil? true})]
        (fact
         res =in=> [#:sort-demo{:num nil}
                    #:sort-demo{:num 1}
                    #:sort-demo{:num 2}
                    #:sort-demo{:num 3}])))

    (testing "Order by and NULLS LAST"
      (let [sort-demo (-> *env*
                          :sort-demo
                          (r/select [:num])
                          (r/order-by [[:num :asc :nulls-last]]))
            ;; We pass {:keep-nil? true} as the decomposition schema
            ;; override because otherwise Penkala will remove rows
            ;; where all primary key values are nil
            res (select! *env* sort-demo {} {:keep-nil? true})]
        (fact
         res =in=> [#:sort-demo{:num 1}
                    #:sort-demo{:num 2}
                    #:sort-demo{:num 3}
                    #:sort-demo{:num nil}])))))

(deftest sql-distinct
  ;; https://www.postgresqltutorial.com/postgresql-select-distinct/
  (testing "Distinct one column"
    (let [distinct-demo (-> *env*
                            :distinct-demo
                            (r/distinct)
                            (r/select [:bcolor])
                            (r/order-by [:bcolor])) ;; add order-by to ensure test consistency
          res (select! *env* distinct-demo {} {:keep-nil? true})]
      (fact
       res =in=> [#:distinct-demo{:bcolor "blue"}
                  #:distinct-demo{:bcolor "green"}
                  #:distinct-demo{:bcolor "red"}
                  #:distinct-demo{:bcolor nil}])))

  (testing "Distinct multiple columns"
    (let [distinct-demo (-> *env*
                            :distinct-demo
                            (r/distinct)
                            (r/select [:bcolor :fcolor])
                            (r/order-by [:bcolor :fcolor])) ;; add order-by to ensure test consistency
          res (select! *env* distinct-demo)]
      (fact
       res =in=> [#:distinct-demo{:bcolor "blue", :fcolor "blue"}
                  #:distinct-demo{:bcolor "blue", :fcolor "green"}
                  #:distinct-demo{:bcolor "blue", :fcolor "red"}
                  #:distinct-demo{:bcolor "green", :fcolor "blue"}
                  #:distinct-demo{:bcolor "green", :fcolor "green"}
                  #:distinct-demo{:bcolor "green", :fcolor "red"}
                  #:distinct-demo{:bcolor "red", :fcolor "blue"}
                  #:distinct-demo{:bcolor "red", :fcolor "green"}
                  #:distinct-demo{:bcolor "red", :fcolor "red"}
                  #:distinct-demo{:bcolor "red", :fcolor nil}
                  #:distinct-demo{:bcolor nil, :fcolor "red"}])))

  (testing "Distinct on"
    (let [distinct-demo (-> *env*
                            :distinct-demo
                            (r/distinct [:bcolor])
                            (r/select [:bcolor :fcolor]))
          res (select! *env* distinct-demo)]
      (fact
       res =in=> [#:distinct-demo{:bcolor "blue"
                                  :fcolor "red"}
                  #:distinct-demo{:bcolor "green"
                                  :fcolor "red"}
                  #:distinct-demo{:bcolor "red"
                                  :fcolor "red"}
                  #:distinct-demo{:bcolor nil
                                  :fcolor "red"}]))))

(deftest where
  ;; https://www.postgresqltutorial.com/postgresql-where/
  (testing "WHERE clause with the equal (=) operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:= :first-name "JAMIE"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "RICE"
                             :first-name "JAMIE"}
                  #:customer{:last-name "WAUGH"
                             :first-name "JAMIE"}])))

  (testing "WHERE clause with the AND operator (1)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:and
                                 [:= :first-name "JAMIE"]
                                 [:= :last-name "RICE"]]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "RICE"
                             :first-name "JAMIE"}])))

  (testing "WHERE clause with the AND operator (2)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:= :first-name "JAMIE"])
                       (r/where [:= :last-name "RICE"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "RICE"
                             :first-name "JAMIE"}])))

  (testing "WHERE clause with the OR operator (1)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:or
                                 [:= :last-name "RODRIGUEZ"]
                                 [:= :first-name "ADAM"]]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "RODRIGUEZ"
                             :first-name "LAURA"}
                  #:customer{:last-name "GOOCH"
                             :first-name "ADAM"}])))

  (testing "WHERE clause with the OR operator (2)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:= :last-name "RODRIGUEZ"])
                       (r/or-where [:= :first-name "ADAM"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "RODRIGUEZ"
                             :first-name "LAURA"}
                  #:customer{:last-name "GOOCH"
                             :first-name "ADAM"}])))

  (testing "WHERE clause with the IN operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:in :first-name ["ANN" "ANNE" "ANNIE"]]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "EVANS"
                             :first-name "ANN"}
                  #:customer{:last-name "POWELL"
                             :first-name "ANNE"}
                  #:customer{:last-name "RUSSELL"
                             :first-name "ANNIE"}])))

  (testing "WHERE clause with the LIKE operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:like :first-name "ANN%"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "HILL"
                             :first-name "ANNA"}
                  #:customer{:last-name "EVANS"
                             :first-name "ANN"}
                  #:customer{:last-name "POWELL"
                             :first-name "ANNE"}
                  #:customer{:last-name "RUSSELL"
                             :first-name "ANNIE"}
                  #:customer{:last-name "OLSON"
                             :first-name "ANNETTE"}])))

  (testing "WHERE clause with the BETWEEN operator"
    (let [customer (-> *env*
                       :customer
                       (r/extend :name-length [:length :first-name])
                       (r/select [:first-name :name-length])
                       (r/where [:and
                                 [:like :first-name "A%"]
                                 [:between :name-length 3 5]])
                       (r/limit 5))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:first-name "AMY"
                             :name-length 3}
                  #:customer{:first-name "ANNA"
                             :name-length 4}
                  #:customer{:first-name "ANN"
                             :name-length 3}
                  #:customer{:first-name "ALICE"
                             :name-length 5}
                  #:customer{:first-name "ANNE"
                             :name-length 4}])))

  (testing "WHERE clause with the NOT EQUAL operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:and
                                 [:like :first-name "BRA%"]
                                 [:<> :last-name "MOTLEY"]]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "GRAVES"
                             :first-name "BRANDY"}
                  #:customer{:last-name "HUEY"
                             :first-name "BRANDON"}
                  #:customer{:last-name "MCCURDY"
                             :first-name "BRAD"}]))))

(deftest limit
  ;; https://www.postgresqltutorial.com/postgresql-limit/
  (testing "Using LIMIT to constrain the number of returned rows"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :release-year])
                   (r/order-by [:film-id])
                   (r/limit 5))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "ACADEMY DINOSAUR"
                         :film-id 1
                         :release-year 2006}
                  #:film{:title "ACE GOLDFINGER"
                         :film-id 2
                         :release-year 2006}
                  #:film{:title "ADAPTATION HOLES"
                         :film-id 3
                         :release-year 2006}
                  #:film{:title "AFFAIR PREJUDICE"
                         :film-id 4
                         :release-year 2006}
                  #:film{:title "AFRICAN EGG"
                         :film-id 5
                         :release-year 2006}])))

  (testing "Using LIMIT with OFFSET"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :release-year])
                   (r/order-by [:film-id])
                   (r/limit 4)
                   (r/offset 3))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "AFFAIR PREJUDICE"
                         :film-id 4
                         :release-year 2006}
                  #:film{:title "AFRICAN EGG"
                         :film-id 5
                         :release-year 2006}
                  #:film{:title "AGENT TRUMAN"
                         :film-id 6
                         :release-year 2006}
                  #:film{:title "AIRPLANE SIERRA"
                         :film-id 7
                         :release-year 2006}])))

  (testing "Using LIMIT / OFFSET to get top / bottom N rows"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :rental-rate])
                   (r/order-by [[:rental-rate :desc]])
                   (r/limit 10))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "AMERICAN CIRCUS"
                         :film-id 21
                         :rental-rate 4.99M}
                  #:film{:title "APACHE DIVINE"
                         :film-id 31
                         :rental-rate 4.99M}
                  #:film{:title "ALI FOREVER"
                         :film-id 13
                         :rental-rate 4.99M}
                  #:film{:title "AMELIE HELLFIGHTERS"
                         :film-id 20
                         :rental-rate 4.99M}
                  #:film{:title "ANTHEM LUKE"
                         :film-id 28
                         :rental-rate 4.99M}
                  #:film{:title "AIRPLANE SIERRA"
                         :film-id 7
                         :rental-rate 4.99M}
                  #:film{:title "AIRPORT POLLOCK"
                         :film-id 8
                         :rental-rate 4.99M}
                  #:film{:title "ACE GOLDFINGER"
                         :film-id 2
                         :rental-rate 4.99M}
                  #:film{:title "ALADDIN CALENDAR"
                         :film-id 10
                         :rental-rate 4.99M}
                  #:film{:title "APOCALYPSE FLAMINGOS"
                         :film-id 32
                         :rental-rate 4.99M}]))))

(deftest fetch
  ;; https://www.postgresqltutorial.com/postgresql-fetch/
  (testing "Using FETCH to constrain the number of returned rows"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :release-year])
                   (r/order-by [:film-id])
                   (r/fetch 5))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "ACADEMY DINOSAUR"
                         :film-id 1
                         :release-year 2006}
                  #:film{:title "ACE GOLDFINGER"
                         :film-id 2
                         :release-year 2006}
                  #:film{:title "ADAPTATION HOLES"
                         :film-id 3
                         :release-year 2006}
                  #:film{:title "AFFAIR PREJUDICE"
                         :film-id 4
                         :release-year 2006}
                  #:film{:title "AFRICAN EGG"
                         :film-id 5
                         :release-year 2006}])))

  (testing "Using FETCH with OFFSET"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :release-year])
                   (r/order-by [:film-id])
                   (r/fetch 4)
                   (r/offset 3))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "AFFAIR PREJUDICE"
                         :film-id 4
                         :release-year 2006}
                  #:film{:title "AFRICAN EGG"
                         :film-id 5
                         :release-year 2006}
                  #:film{:title "AGENT TRUMAN"
                         :film-id 6
                         :release-year 2006}
                  #:film{:title "AIRPLANE SIERRA"
                         :film-id 7
                         :release-year 2006}])))

  (testing "Using FETCH / OFFSET to get top / bottom N rows"
    (let [film (-> *env*
                   :film
                   (r/select [:film-id :title :rental-rate])
                   (r/order-by [[:rental-rate :desc]])
                   (r/fetch 10))
          res (select! *env* film)]
      (fact
       res =in=> [#:film{:title "AMERICAN CIRCUS"
                         :film-id 21
                         :rental-rate 4.99M}
                  #:film{:title "APACHE DIVINE"
                         :film-id 31
                         :rental-rate 4.99M}
                  #:film{:title "ALI FOREVER"
                         :film-id 13
                         :rental-rate 4.99M}
                  #:film{:title "AMELIE HELLFIGHTERS"
                         :film-id 20
                         :rental-rate 4.99M}
                  #:film{:title "ANTHEM LUKE"
                         :film-id 28
                         :rental-rate 4.99M}
                  #:film{:title "AIRPLANE SIERRA"
                         :film-id 7
                         :rental-rate 4.99M}
                  #:film{:title "AIRPORT POLLOCK"
                         :film-id 8
                         :rental-rate 4.99M}
                  #:film{:title "ACE GOLDFINGER"
                         :film-id 2
                         :rental-rate 4.99M}
                  #:film{:title "ALADDIN CALENDAR"
                         :film-id 10
                         :rental-rate 4.99M}
                  #:film{:title "APOCALYPSE FLAMINGOS"
                         :film-id 32
                         :rental-rate 4.99M}]))))

(deftest in
  ;; https://www.postgresqltutorial.com/postgresql-in/
  (testing "IN Operator"
    (let [rental (-> *env*
                     :rental
                     (r/select [:customer-id :rental-id :return-date])
                     (r/where [:in :customer-id [1 2]]))
          res (-> (select! *env* rental)
                  (subvec 0 5))]
      (fact
       res =in=> [#:rental{:customer-id 1
                           :return-date datetime?
                           :rental-id 76}
                  #:rental{:customer-id 2
                           :return-date datetime?
                           :rental-id 320}
                  #:rental{:customer-id 1
                           :return-date datetime?
                           :rental-id 573}
                  #:rental{:customer-id 1
                           :return-date datetime?
                           :rental-id 1185}
                  #:rental{:customer-id 1
                           :return-date datetime?
                           :rental-id 1422}]))

    (testing "Emulating IN with OR"
      (let [rental (-> *env*
                       :rental
                       (r/select [:customer-id :rental-id :return-date])
                       (r/where [:or
                                 [:= :customer-id 1]
                                 [:= :customer-id 2]]))
            res (-> (select! *env* rental)
                    (subvec 0 5))]
        (fact
         res =in=> [#:rental{:customer-id 1
                             :return-date datetime?
                             :rental-id 76}
                    #:rental{:customer-id 2
                             :return-date datetime?
                             :rental-id 320}
                    #:rental{:customer-id 1
                             :return-date datetime?
                             :rental-id 573}
                    #:rental{:customer-id 1
                             :return-date datetime?
                             :rental-id 1185}
                    #:rental{:customer-id 1
                             :return-date datetime?
                             :rental-id 1422}]))))

  (testing "NOT IN Operator"
    (let [rental (-> *env*
                     :rental
                     (r/select [:customer-id :rental-id :return-date])
                     (r/where [:not-in :customer-id [1 2]]))
          res (-> (select! *env* rental)
                  (subvec 0 5))]
      (fact
       res =in=> [#:rental{:customer-id 459
                           :return-date datetime?
                           :rental-id 2}
                  #:rental{:customer-id 408
                           :return-date datetime?
                           :rental-id 3}
                  #:rental{:customer-id 333
                           :return-date datetime?
                           :rental-id 4}
                  #:rental{:customer-id 222
                           :return-date datetime?
                           :rental-id 5}
                  #:rental{:customer-id 549
                           :return-date datetime?
                           :rental-id 6}]))

    (testing "Emulating NOT IN with AND"
      (let [rental (-> *env*
                       :rental
                       (r/select [:customer-id :rental-id :return-date])
                       (r/where [:and
                                 [:<> :customer-id 1]
                                 [:<> :customer-id 2]]))
            res (-> (select! *env* rental)
                    (subvec 0 5))]
        (fact
         res =in=> [#:rental{:customer-id 459
                             :return-date datetime?
                             :rental-id 2}
                    #:rental{:customer-id 408
                             :return-date datetime?
                             :rental-id 3}
                    #:rental{:customer-id 333
                             :return-date datetime?
                             :rental-id 4}
                    #:rental{:customer-id 222
                             :return-date datetime?
                             :rental-id 5}
                    #:rental{:customer-id 549
                             :return-date datetime?
                             :rental-id 6}]))))

  (testing "IN with a subquery"
    (let [rental-customers (-> *env*
                               :rental
                               (r/select [:customer-id])
                               (r/where [:=
                                         [:cast "2005-05-27" "date"]
                                         [:cast :return-date "date"]]))
          rental (-> *env*
                     :rental
                     (r/select [:customer-id :rental-id :return-date])
                     (r/where [:in :customer-id rental-customers]))
          res (-> (select! *env* rental)
                  (subvec 0 5))]
      (fact
       res =in=> [#:rental{:customer-id 549
                           :return-date datetime?
                           :rental-id 6}
                  #:rental{:customer-id 269
                           :return-date datetime?
                           :rental-id 7}
                  #:rental{:customer-id 575
                           :return-date datetime?
                           :rental-id 17}
                  #:rental{:customer-id 185
                           :return-date datetime?
                           :rental-id 20}
                  #:rental{:customer-id 350
                           :return-date datetime?
                           :rental-id 24}]))))

(deftest between
  ;; https://www.postgresqltutorial.com/postgresql-between/

  (testing "BETWEEN operator"
    (let [payment (-> *env*
                      :payment
                      (r/select [:customer-id :payment-id :amount])
                      (r/where [:between :amount 8 9]))
          res (-> (select! *env* payment)
                  (subvec 0 5))]
      (fact
       res =in=> [#:payment{:customer-id 271
                            :amount 8.99M
                            :payment-id 16058}
                  #:payment{:customer-id 293
                            :amount 8.99M
                            :payment-id 16102}
                  #:payment{:customer-id 299
                            :amount 8.99M
                            :payment-id 16113}
                  #:payment{:customer-id 315
                            :amount 8.99M
                            :payment-id 16154}
                  #:payment{:customer-id 316
                            :amount 8.99M
                            :payment-id 16157}])))

  (testing "NOT BETWEEN operator"
    (let [payment (-> *env*
                      :payment
                      (r/select [:customer-id :payment-id :amount])
                      (r/where [:not [:between :amount 8 9]]))
          res (-> (select! *env* payment)
                  (subvec 0 5))]
      (fact
       res =in=> [#:payment{:customer-id 269
                            :amount 1.99M
                            :payment-id 16050}
                  #:payment{:customer-id 269
                            :amount 0.99M
                            :payment-id 16051}
                  #:payment{:customer-id 269
                            :amount 6.99M
                            :payment-id 16052}
                  #:payment{:customer-id 269
                            :amount 0.99M
                            :payment-id 16053}
                  #:payment{:customer-id 269
                            :amount 4.99M
                            :payment-id 16054}])))

  (testing "BETWEEN dates"
    (let [payment (-> *env*
                      :payment
                      (r/select [:customer-id :payment-id :payment-date :amount])
                      (r/where [:between :payment-date [:cast "2020-02-07" "date"] [:cast "2020-02-15" "date"]]))
          res (-> (select! *env* payment)
                  (subvec 0 5))]
      (fact
       res =in=> [#:payment{:customer-id 284
                            :amount 0.99M
                            :payment-date datetime?
                            :payment-id 17270}
                  #:payment{:customer-id 285
                            :amount 7.99M
                            :payment-date datetime?
                            :payment-id 17273}
                  #:payment{:customer-id 306
                            :amount 0.99M
                            :payment-date datetime?
                            :payment-id 17356}
                  #:payment{:customer-id 310
                            :amount 4.99M
                            :payment-date datetime?
                            :payment-id 17373}
                  #:payment{:customer-id 323
                            :amount 0.99M
                            :payment-date datetime?
                            :payment-id 17424}]))))

(deftest like
  ;; https://www.postgresqltutorial.com/postgresql-like/

  (testing "LIKE operator (1)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:like :first-name "JEN%"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "DAVIS"
                             :first-name "JENNIFER"}
                  #:customer{:last-name "TERRY"
                             :first-name "JENNIE"}
                  #:customer{:last-name "CASTRO"
                             :first-name "JENNY"}])))

  (testing "LIKE operator (2)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:like :first-name "%ER%"]))
          res (-> (select! *env* customer)
                  (subvec 0 5))]
      (fact
       res =in=> [#:customer{:last-name "DAVIS"
                             :first-name "JENNIFER"}
                  #:customer{:last-name "LEE"
                             :first-name "KIMBERLY"}
                  #:customer{:last-name "CAMPBELL"
                             :first-name "CATHERINE"}
                  #:customer{:last-name "MORRIS"
                             :first-name "HEATHER"}
                  #:customer{:last-name "ROGERS"
                             :first-name "TERESA"}])))

  (testing "LIKE operator (3)"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:like :first-name "_HER%"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "MURPHY"
                             :first-name "CHERYL"}
                  #:customer{:last-name "WATSON"
                             :first-name "THERESA"}
                  #:customer{:last-name "MARSHALL"
                             :first-name "SHERRY"}
                  #:customer{:last-name "RHODES"
                             :first-name "SHERRI"}])))

  (testing "NOT LIKE operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:not-like :first-name "JEN%"]))
          res (-> (select! *env* customer)
                  (subvec 0 5))]
      (fact
       res =in=> [#:customer{:last-name "SMITH"
                             :first-name "MARY"}
                  #:customer{:last-name "JOHNSON"
                             :first-name "PATRICIA"}
                  #:customer{:last-name "WILLIAMS"
                             :first-name "LINDA"}
                  #:customer{:last-name "JONES"
                             :first-name "BARBARA"}
                  #:customer{:last-name "BROWN"
                             :first-name "ELIZABETH"}])))

  (testing "ILIKE operator"
    (let [customer (-> *env*
                       :customer
                       (r/select [:first-name :last-name])
                       (r/where [:ilike :first-name "bar%"]))
          res (select! *env* customer)]
      (fact
       res =in=> [#:customer{:last-name "JONES"
                             :first-name "BARBARA"}
                  #:customer{:last-name "LOVELACE"
                             :first-name "BARRY"}]))))

(deftest is-null
  ;; https://www.postgresqltutorial.com/postgresql-is-null/

  (testing "IS NULL operator"
    (let [contacts (-> *env*
                       :contacts
                       (r/select [:id :first-name :last-name :email :phone])
                       (r/where [:is-null :phone]))
          res (select! *env* contacts)]
      (fact
       res =in=> [#:contacts{:email "john.doe@example.com"
                             :last-name "Doe"
                             :phone nil
                             :first-name "John"
                             :id 1}])))

  (testing "IS NOT NULL operator"
    (let [contacts (-> *env*
                       :contacts
                       (r/select [:id :first-name :last-name :email :phone])
                       (r/where [:is-not-null :phone]))
          res (select! *env* contacts)]
      (fact
       res =in=> [#:contacts{:email "lily.bush@example.com"
                             :last-name "Bush"
                             :phone "(408-234-2764)"
                             :first-name "Lily"
                             :id 2}]))))

(deftest inner-join
  ;; https://www.postgresqltutorial.com/postgresql-inner-join/

  (testing "INNER JOIN two tables"
    (let [payment (-> *env*
                      :payment
                      (r/select [:amount :payment-date :customer-id]))
          customer (-> *env*
                       :customer
                       (r/select [:customer-id :first-name :last-name])
                       (r/join :inner payment :payments [:= :customer-id :payments/customer-id])
                       (r/order-by [:payments/payment-date]))
          res (-> (select! *env* customer)
                  (subvec 0 2))]
      (fact
       res =in=> [#:customer{:customer-id 130
                             :last-name "HUNTER"
                             :first-name "CHARLOTTE"
                             :payments [#:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 3.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?}]}
                  #:customer{:customer-id 459
                             :last-name "COLLAZO"
                             :first-name "TOMMY"
                             :payments [#:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 9.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 7.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 7.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 10.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 5.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 5.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 9.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 10.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?}]}]))
    (testing "LEFT LATERAL JOIN two tables"
      ;; This example expands on the previous one (not present on the https://www.postgresqltutorial.com/postgresql-inner-join page)

      ;; Instead of getting all customers with all payments, we'll get first two customers each with first two
      ;; payments. This will require a bit more work, but is a good example of using more advanced composability
      ;; features of Penkala.

      (let [customer (:customer *env*)
            payment (:payment *env*)
            ;; First we want to get the customer ids for customer which have payment records. We want to get 
            ;; the first two customer ids from payments ordered by payment date (so customer-ids that made the)
            ;; first payments. Since we need two *different* ids (a customer could make multiple payments in a row)
            ;; we use the window function to partition payments by customer-ids and then get rows where row-number is 1.
            ;; `(r/wrap relation)` function used here forces another subselect layer so we can select only the
            ;; `:customer-id` column from the outer layer. This allows us to pass this subselect to the IN operation
            ;; in the final query.
            customers-with-payments (-> payment
                                        (r/extend-with-window :row-number [:row-number] [:customer-id] [:payment-date])
                                        (r/select [:customer-id :row-number])
                                        (r/order-by [:payment-date])
                                        (r/wrap)
                                        (r/where [:= :row-number 1])
                                        (r/select [:customer-id])
                                        (r/limit 2))

            ;; This relation will be laft lateral joined to the parent relation, so we'll get
            ;; two payments per customer. Notice `(r/with-parent relation customer)` and `[:parent-scope :customer-id]`
            ;; - this allows the inner subquery to access the columns from the parent select without having
            ;; to care about the aliasing performed by Penkala. For each customer we'll get two payments
            payments-for-customer (-> payment
                                      (r/select [:amount :payment-date :customer-id])
                                      (r/with-parent customer)
                                      (r/order-by [:payment-date])
                                      (r/where [:= :customer-id [:parent-scope :customer-id]])
                                      (r/limit 2))

            ;; Final relation - we select customers where ids are in the result of the `customers-with-payments` query
            ;; and we left lateral join `payments-for-customer` which will select two payments for each customer
            customer (-> customer
                         (r/join :left-lateral payments-for-customer :payments true)
                         (r/where [:in :customer-id customers-with-payments])
                         (r/select [:customer-id :first-name :last-name])
                         (r/order-by [:payments/payment-date]))

            res  (select! *env* customer)]
        (fact
         res =in=> [#:customer{:customer-id 130
                               :last-name "HUNTER"
                               :first-name "CHARLOTTE"
                               :payments [#:payment{:customer-id 130
                                                    :amount 2.99M
                                                    :payment-date datetime?}
                                          #:payment{:customer-id 130
                                                    :amount 2.99M
                                                    :payment-date datetime?}]}
                    #:customer{:customer-id 459
                               :last-name "COLLAZO"
                               :first-name "TOMMY"
                               :payments [#:payment{:customer-id 459
                                                    :amount 2.99M
                                                    :payment-date datetime?}
                                          #:payment{:customer-id 459
                                                    :amount 0.99M
                                                    :payment-date datetime?}]}]))))

  (testing "INNER JOIN three tables"
    (let [staff (-> *env*
                    :staff
                    (r/select [:first-name :last-name :staff-id]))

          payment (-> *env*
                      :payment
                      (r/select [:amount :payment-date :customer-id])
                      (r/join :inner staff :staff [:= :staff-id :staff/staff-id]))

          customer (-> *env*
                       :customer
                       (r/select [:customer-id :first-name :last-name])
                       (r/join :inner payment :payments [:= :customer-id :payments/customer-id])
                       (r/order-by [:payments/payment-date]))
          res (-> (select! *env* customer)
                  (subvec 0 2))]
      (fact
       res =in=> [#:customer{:customer-id 130
                             :last-name "HUNTER"
                             :first-name "CHARLOTTE"
                             :payments [#:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 3.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 130
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 5.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 130
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}]}
                  #:customer{:customer-id 459
                             :last-name "COLLAZO"
                             :first-name "TOMMY"
                             :payments [#:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 9.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 7.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 7.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 10.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 5.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 0.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 3.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 5.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 9.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 10.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 2.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 6.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Hillyer"
                                                                  :first-name "Mike"
                                                                  :staff-id 1}]}
                                        #:payment{:customer-id 459
                                                  :amount 4.99M
                                                  :payment-date datetime?
                                                  :staff [#:staff{:last-name "Stephens"
                                                                  :first-name "Jon"
                                                                  :staff-id 2}]}]}]))))

(deftest left-join
  ;; https://www.postgresqltutorial.com/postgresql-left-join/

  (testing "LEFT JOIN (1)"
    (let [inventory (-> *env*
                        :inventory
                        (r/select [:film-id :inventory-id]))
          film (-> *env*
                   :film
                   ;; Last argument is join projection - `inventory` relation exposes two columns - `film-id` and
                   ;; `inventory-id` but we're selecting only `inventory-id` from the joined relation
                   (r/join :left inventory :inventory [:= :film-id :inventory/film-id] [:inventory/inventory-id])
                   (r/order-by [:title :inventory/inventory-id])
                   (r/select [:film-id :title]))
          res (-> (select! *env* film)
                  (subvec 0 2))]
      (fact
       res =in=> [#:film{:title "ACADEMY DINOSAUR"
                         :film-id 1
                         :inventory [#:inventory{:inventory-id 1}
                                     #:inventory{:inventory-id 2}
                                     #:inventory{:inventory-id 3}
                                     #:inventory{:inventory-id 4}
                                     #:inventory{:inventory-id 5}
                                     #:inventory{:inventory-id 6}
                                     #:inventory{:inventory-id 7}
                                     #:inventory{:inventory-id 8}]}
                  #:film{:title "ACE GOLDFINGER"
                         :film-id 2
                         :inventory [#:inventory{:inventory-id 9}
                                     #:inventory{:inventory-id 10}
                                     #:inventory{:inventory-id 11}]}])))

  (testing "LEFT JOIN (2)"
    (let [inventory (-> *env*
                        :inventory
                        (r/select [:film-id :inventory-id]))
          film (-> *env*
                   :film
                   (r/join :left inventory :inventory [:= :film-id :inventory/film-id] [:inventory/inventory-id])
                   (r/where [:is-null :inventory/film-id])
                   (r/select [:film-id :title])
                   (r/order-by [:title]))
          res (-> (select! *env* film)
                  (subvec 0 2))]
      (fact
       res =in=> [#:film{:title "ALICE FANTASIA"
                         :film-id 14
                         :inventory []}
                  #:film{:title "APOLLO TEEN"
                         :film-id 33
                         :inventory []}]))))

(deftest right-join
  ;; https://www.postgresqltutorial.com/postgresql-right-join/
  (testing "RIGHT JOIN (1)"
    (let [film-reviews (-> *env*
                           :film-reviews
                           (r/select [:film-id :review]))
          films (-> *env*
                    :films
                    (r/select [:title])
                    (r/join :right film-reviews :film-reviews [:using :film-id] [:film-reviews/review]))
        ;; Last argument is the decomposition schema. Right joins can't be decomposed correctly if the "left"
        ;; table is missing values, so we're passing false to get unprocessed list of results
          res (select! *env* films nil false)]
      (fact
       res =in=> [{:title "Joker", :film-reviews/review "Excellent"}
                  {:title "Joker", :film-reviews/review "Awesome"}
                  {:title "Avengers: Endgame", :film-reviews/review "Cool"}
                  {:title nil, :film-reviews/review "Beautiful"}])))
  (testing "RIGHT JOIN (2)"
    (let [film-reviews (-> *env*
                           :film-reviews
                           (r/select [:film-id :review]))
          films (-> *env*
                    :films
                    (r/select [:title])
                    (r/where [:is-null :title])
                    (r/join :right film-reviews :film-reviews [:using :film-id] [:film-reviews/review]))
        ;; Last argument is the decomposition schema. Right joins can't be decomposed correctly if the "left"
        ;; table is missing values, so we're passing false to get unprocessed list of results
          res (select! *env* films nil false)]
      (fact
       res =in=> [{:title nil, :film-reviews/review "Beautiful"}]))))

(deftest self-join
  ;; https://www.postgresqltutorial.com/postgresql-self-join/
  (testing "1 - Querying hierarchical data example"
    (let [manager (-> *env*
                      :employee
                      (r/extend :manager [:concat :first-name " " :last-name]))
          employee (-> *env*
                       :employee
                       (r/extend :employee [:concat :first-name " " :last-name]))
          hierarchy (-> employee
                        (r/join :left manager :manager [:= :manager-id :manager/employee-id] [:manager/manager])
                        (r/select [:employee])
                        (r/order-by [:manager/manager]))
        ;; Last argument is the decomposition schema. We're passing false to get a flat list of results
          res (select! *env* hierarchy nil false)]
      (fact
       res =in=> [{:employee "Windy Hays", :manager/manager nil}
                  {:employee "Sau Norman", :manager/manager "Ava Christensen"}
                  {:employee "Anna Reeves", :manager/manager "Ava Christensen"}
                  {:employee "Salley Lester", :manager/manager "Hassan Conner"}
                  {:employee "Kelsie Hays", :manager/manager "Hassan Conner"}
                  {:employee "Tory Goff", :manager/manager "Hassan Conner"}
                  {:employee "Ava Christensen", :manager/manager "Windy Hays"}
                  {:employee "Hassan Conner", :manager/manager "Windy Hays"}])))

  (testing "2 - Comparing the rows with the same table"
    (let [film (:film *env*)
          films (-> film
                    (r/join :inner film :film-2 [:and [:<> :film-id :film-2/film-id] [:= :length :film-2/length]] [:film-2/title])
                    (r/select [:title :length])
                    (r/order-by [:title]))
        ;; Last argument is the decomposition schema. We're passing false to get a flat list of results
          res (-> (select! *env* films nil false)
                  (subvec 0 10))]
      (fact
       res =in=> [{:title "ACADEMY DINOSAUR", :length 86, :film-2/title "YENTL IDAHO"}
                  {:title "ACADEMY DINOSAUR", :length 86, :film-2/title "ANNIE IDENTITY"}
                  {:title "ACADEMY DINOSAUR", :length 86, :film-2/title "MIDNIGHT WESTWARD"}
                  {:title "ACADEMY DINOSAUR", :length 86, :film-2/title "GANDHI KWAI"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "RUSH GOODFELLAS"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "PELICAN COMFORTS"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "PARADISE SABRINA"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "ODDS BOOGIE"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "MIDSUMMER GROUNDHOG"}
                  {:title "ACE GOLDFINGER", :length 48, :film-2/title "HEAVEN FREEDOM"}]))))

(deftest full-outer-join
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-full-outer-join/
  (testing "Full outer join"
    (let [departments (:departments *env*)
          employees (-> *env*
                        :employees
                        (r/join :full departments :departments [:= :department-id :departments/department-id]))
          res (select! *env* employees nil false)]
      (fact
       res =in=> [{:department-id 1, :employee-name "Bette Nicholson", :employee-id 1, :departments/department-id 1, :departments/department-name "Sales"}
                  {:department-id 1, :employee-name "Christian Gable", :employee-id 2, :departments/department-id 1, :departments/department-name "Sales"}
                  {:department-id 2, :employee-name "Joe Swank", :employee-id 3, :departments/department-id 2, :departments/department-name "Marketing"}
                  {:department-id 3, :employee-name "Fred Costner", :employee-id 4, :departments/department-id 3, :departments/department-name "HR"}
                  {:department-id 4, :employee-name "Sandra Kilmer", :employee-id 5, :departments/department-id 4, :departments/department-name "IT"}
                  {:department-id nil, :employee-name "Julia Mcqueen", :employee-id 6, :departments/department-id nil, :departments/department-name nil}
                  {:department-id nil, :employee-name nil, :employee-id nil, :departments/department-id 5, :departments/department-name "Production"}])))
  (testing "Full outer join - find departments without employees"
    (let [departments (:departments *env*)
          employees (-> *env*
                        :employees
                        (r/join :full departments :departments [:= :department-id :departments/department-id])
                        (r/where [:is-null :employee-name]))
          res (select! *env* employees nil false)]
      (fact
       res =in=> [{:department-id nil, :employee-name nil, :employee-id nil, :departments/department-id 5, :departments/department-name "Production"}])))
  (testing "Full outer join - find employees without department"
    (let [departments (:departments *env*)
          employees (-> *env*
                        :employees
                        (r/join :full departments :departments [:= :department-id :departments/department-id])
                        (r/where [:is-null :departments/department-name]))
          res (select! *env* employees nil false)]
      (fact
       res =in=> [{:department-id nil, :employee-name "Julia Mcqueen", :employee-id 6, :departments/department-id nil, :departments/department-name nil}]))))

(deftest cross-join
  ;; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-full-outer-join/
  (let [t-2 (:t-2 *env*)
        t-1 (-> *env*
                :t-1
                (r/join :cross t-2 :t-2 nil))
        res (select! *env* t-1 nil false)]
    (fact
     res =in=> [{:label "A", :t-2/score 1}
                {:label "B", :t-2/score 1}
                {:label "A", :t-2/score 2}
                {:label "B", :t-2/score 2}
                {:label "A", :t-2/score 3}
                {:label "B", :t-2/score 3}])))