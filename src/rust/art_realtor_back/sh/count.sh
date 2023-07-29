#/usr/bin/env bash
facet="msk_habit"
fetch_limit=1
fields='[ "guid" ]'
query='{
      "bool": {
        "must": [
          {
            "range": {
              "pub_datetime": {
                  "gte": "now-7d/d" /* за последние 7 дней */
              }
            }
          },
          {
              "term": {
                  "deal_status_id": 1 /* Только актуальные */
              }
          },
          {
              "terms": {
                  "media_id": [
                      3 /* WinNER - зелёная зона */, 
                      24 /* WinNER - белая зона */, 
                      15 /* Sob.ru */,
                      17 /* Cian */ ,
                      21 /* Avito */,
                      23 /* Яндекс */,
                      30 /* radver.ru */,
                      31 /* afy.ru */,
                      32 /* Домклик */,
                      33 /* Прочие (Д) */
                  ]
              }
          }
        ]
      }
    }'
                      # 3 /* WinNER - зелёная зона */, 
                      # 24 /* WinNER - белая зона */, 
                      # 17 /* Cian */ 
url="http://stable-mls-search2.baza-winner.ru:9200/${facet}_advs/_search"
header='Content-Type: application/json'
data='{
    "size": '"$fetch_limit"',
    "_source": '"$fields"',
    "query": '"$query"'
}'
curl -X POST "$url" -H "$header" -d "$data" | jq .
