# wtd

Wiki table downloader

## Description

A toy CLI tool for scraping tables off of wikipedia and putting them into sqlite3 databases.

## Development

* Ensure you have sqlite3 installed then try running `./test.sh` which will build, test, and insert a few tables into a db

## Still in development

This project is missing many features. It likely will not work on all but the most simple of wikipedia pages (those with one table and no tables inside of tables) as many pages have different layouts and formatting that make scraping the data difficult.

* Removing most of the `std::process::exit()` calls with custom errors
* Fix for pages that have multiple tables.
* Getting titles from table captions or the closest header
* Fix for tables in tables
* More tests.
