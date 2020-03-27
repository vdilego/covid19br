# needs jq -> sudo apt install jq
curl -s 'https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalMapa' \
-H 'X-Parse-Application-Id: unAFkcaNDeXajurGB7LChj8SgQYS2ptm' \
-H 'TE: Trailers' | jq -r '.results | (map(keys) | add | unique) as $cols | map(. as $row | $cols | map($row[.])) as $rows | $cols, $rows[] | @csv'
