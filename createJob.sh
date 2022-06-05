#!/bin/sh
read -p  "JOB Name[Eg:my_etl_job] :" JOB
mkdir -p "jobs/$JOB"
mkdir -p "jobs/$JOB/scripts"
mkdir -p "jobs/$JOB/sql"
mkdir -p "jobs/$JOB/jar"
mkdir -p "jobs/$JOB/pyscripts"
mkdir -p "jobs/$JOB/config"
mkdir -p "jobs/$JOB/source"
mkdir -p "jobs/$JOB/resources"

