#!/bin/bash
# RadaCrypto Local Dashboard Server
# Usage: bash serve.sh
echo "🚀 RadaCrypto Dashboard — http://localhost:8686"
python3 -m http.server 8686 --directory docs
