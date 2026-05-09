#!/bin/bash
# رنگ‌ها برای خروجی زیبا
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "Installing pq-rs..."


curl -L https://github.com/YOUR_USERNAME/pq-rs/releases/latest/download/pq-rs -o pq

if [ $? -ne 0 ]; then
    echo -e "${RED}Download failed!${NC}"
    exit 1
fi

chmod +x pq

sudo mv pq /usr/local/bin/

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ pq-rs installed successfully!${NC}"
    echo "Now you can use: pq schema file.parquet"
else
    echo -e "${RED}Installation failed!${NC}"
    exit 1
fi