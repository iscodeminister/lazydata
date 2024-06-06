#!/bin/bash

display_menu() {
  echo "Please choose your destiny:"
  echo "1) one key MLlib"
  echo "2) Exit"
}

download_file() {
  local file_url="$1"
  local file_name=$(basename "$file_url")
  echo "Downloading $file_name..."
  wget "$file_url"
  echo "Downloaded $file_name"
}

GITHUB_BASE_URL="https://raw.githubusercontent.com/iscodeminister/lazydata/main"

while true; do
  display_menu
  read -p "Enter your choice: " choice
  case $choice in
    1)
      mkdir mllib
      cd mllib
      hdfs dfs -mkdir -p /data
      download_file "$GITHUB_BASE_URL/mllib/test.csv"
      download_file "$GITHUB_BASE_URL/mllib/train.csv"
      hdfs dfs -put * /data
      cd ~
      download_file "$GITHUB_BASE_URL/pipeline.py"
      spark-submit --master local pipeline.py
      ;;
    2)
      echo "Exiting..."
      break
      ;;
    *)
      echo "Invalid choice. Please try again."
      ;;
  esac
done
