for f in html_files/cache/*.json; do jq -r ".data[][]" "$f" 2>/dev/null | grep -i "greenleys sovereigns.*mursley merlins.*2024.*10.*15" || true; done
