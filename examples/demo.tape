# VHS tape file for kbridge demo
# Run with: ./scripts/generate-demo.sh
# Requires local-dev setup (docker compose)

Output examples/demo.gif

Set FontSize 14
Set Width 1000
Set Height 600
Set LineHeight 1.4
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful

Type "kbridge fetch -b localhost:9092 -p group.id=kbridge > source_offsets.csv"
Enter
Wait@60s

Type "cat source_offsets.csv"
Enter
Sleep 3s

Type "kbridge calculate -b localhost:9093 -H Offset -i source_offsets.csv -p group.id=kbridge > target_offsets.csv"
Enter
Hide
Wait@60s
Show

Type "kbridge apply -b localhost:9093 -i target_offsets.csv -p group.id=kbridge"
Enter
Sleep 2s

Type "Y"
Enter
Wait@30s
Sleep 5s
