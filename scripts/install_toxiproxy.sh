#!/bin/bash
set -e

# Install toxiproxy for network fault injection
if ! command -v toxiproxy-server &> /dev/null; then
    echo "Installing toxiproxy..."

    # Detect OS
    OS="$(uname -s)"
    ARCH="$(uname -m)"

    case "$OS" in
        Darwin)
            # macOS
            if [ "$ARCH" = "arm64" ]; then
                curl -L https://github.com/Shopify/toxiproxy/releases/download/v2.5.0/toxiproxy-server-2.5.0-darwin-arm64 -o /tmp/toxiproxy
            else
                curl -L https://github.com/Shopify/toxiproxy/releases/download/v2.5.0/toxiproxy-server-2.5.0-darwin-amd64 -o /tmp/toxiproxy
            fi
            ;;
        Linux)
            # Linux
            if [ "$ARCH" = "aarch64" ]; then
                curl -L https://github.com/Shopify/toxiproxy/releases/download/v2.5.0/toxiproxy-server-2.5.0-linux-arm64 -o /tmp/toxiproxy
            else
                curl -L https://github.com/Shopify/toxiproxy/releases/download/v2.5.0/toxiproxy-cli-2.5.0-linux-amd64 -o /tmp/toxiproxy
            fi
            ;;
        *)
            echo "Unsupported OS: $OS"
            exit 1
            ;;
    esac

    chmod +x /tmp/toxiproxy
    sudo mv /tmp/toxiproxy /usr/local/bin/toxiproxy-server
    echo "✓ toxiproxy installed"
else
    echo "✓ toxiproxy already installed"
fi
