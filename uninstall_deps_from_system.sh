#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

if [ "$EUID" -ne 0 ]; then
    print_error "请以 root 权限运行此脚本 (sudo)"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPS_DIR="$SCRIPT_DIR"

# 卸载头文件
uninstall_headers() {
    print_info "卸载头文件..."

    if [ -d "$DEPS_DIR/includes" ] && [ -n "$(ls -A "$DEPS_DIR/includes")" ]; then
        for file in "$DEPS_DIR/includes"/*; do
            filename=$(basename "$file")
            if [ -e "/usr/include/$filename" ]; then
                rm -rf "/usr/include/$filename"
                print_info "已删除: /usr/include/$filename"
            fi
        done
    fi
}

# 卸载库文件
uninstall_libs() {
    print_info "卸载库文件..."

    if [ -d "$DEPS_DIR/libs" ] && [ -n "$(ls -A "$DEPS_DIR/libs")" ]; then
        for file in "$DEPS_DIR/libs"/*; do
            filename=$(basename "$file")
            if [ -e "/usr/lib/$filename" ]; then
                rm -f "/usr/lib/$filename"
                print_info "已删除: /usr/lib/$filename"
            fi
        done

        if command -v ldconfig &>/dev/null; then
            ldconfig
        fi
    fi
}

print_info "开始卸载..."
uninstall_headers
uninstall_libs
print_success "卸载依赖完成！"
