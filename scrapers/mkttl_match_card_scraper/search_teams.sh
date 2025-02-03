for f in html_files/cache/*.json; do echo "=== $f ==="; jq -r ".data[]" "$f" 2>/dev/null | grep -i "sovereigns.*merlins\|merlins.*sovereigns" || true; done
