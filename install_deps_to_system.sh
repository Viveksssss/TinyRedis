#!/bin/bash

if [ "$EUID" -ne 0 ]; then
    echo "请使用 sudo 运行"
    exit 1
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "复制头文件到 /usr/include..."
cp -rv "$DIR/includes/"* /usr/include/

echo "复制库文件到 /usr/lib..."
cp -rv "$DIR/libs/"* /usr/lib/

ldconfig

echo "安装依赖完毕"
