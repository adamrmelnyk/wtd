#!/usr/bin/env bash
# A bash test tool for testing the scraper functionality

cargo build;
cargo test;
if [ $? -eq 0 ]; then
    ./target/debug/wtd 'https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population'
    ./target/debug/wtd 'https://en.wikipedia.org/wiki/Member_states_of_the_United_Nations'
    sqlite3 wikiDatabase.db 'SELECT * FROM List_of_countries_and_dependencies_by_population WHERE Rank = 1;'
    sqlite3 wikiDatabase.db 'DROP TABLE List_of_countries_and_dependencies_by_population;'
    sqlite3 wikiDatabase.db 'DROP TABLE Member_states_of_the_United_Nations;'
    if [ $? -eq 0 ]; then
        echo "SUCCESS";
    else
        echo "FAIL";
    fi
else
    echo "FAIL";
fi
