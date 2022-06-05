#!/bin/sh
read -p  "DOMAIN/TABLE Name[Eg:my_table] :" DOMAIN
mkdir -p "data/$DOMAIN"
mkdir -p "data/$DOMAIN/raw"
mkdir -p "data/$DOMAIN/processing"

