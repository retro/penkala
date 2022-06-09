# Change Log

## [0.9.1] - 2022-06-09

- Fix regression where updating to nil value was generating incorrect SQL
- Implement support for explicit ON CONFLICT update WHERE clause

## [0.9.0] - 2022-06-08

- Implement CTEs
- Implement support for ordering in aggregate functions
- Implement support for array literal
- Implement tests based on the queries found on https://www.postgresqltutorial.com
- Implement explicit join API functions
- Implement explicit GROUP BY, GROUPING SETS, CUBE and ROLLUP
- Implement set operations in value expressions

## [0.1.0] - 2021-05-26

- Penkala will now throw an exception if a keyword can't be matched to a column. Previously keyword was treated as a value which made it easy to write a query that looked correct, but had a wrong behavior.
