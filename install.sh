#!/bin/bash

display_menu() {
  echo "Please choose a file to download from GitHub:"
  echo "1) File 1"
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

FILE1_URL="$GITHUB_BASE_URL/path/to/file1"

while true; do
  display_menu
  
  read -p "Enter your choice: " choice
  
  case $choice in
    1)
      download_file "$FILE1_URL"
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
