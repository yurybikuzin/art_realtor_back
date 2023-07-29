Данные нужны по региону Москва и МО. Разделы продажа вторичка и продажа загородная.

## Поля Вторичка:

- адрес
  - "geo_cache_country_name",
  - "geo_cache_state_name",
  - "geo_cache_settlement_name",
  - "geo_cache_town_name_2",
  - "geo_cache_estate_object_name",
  - "geo_cache_district_name",
  - "geo_cache_region_name",
  - "geo_cache_highway_name_1",
  - "geo_cache_street_name",
  - "geo_cache_building_name",
- метро
  - "geo_cache_subway_station_name_1",
  - "geo_cache_subway_station_name_2",
  - "geo_cache_subway_station_name_3",
  - "geo_cache_subway_station_name_4",
- от метро
  - "walking_access_1",
  - "walking_access_2",
  - "walking_access_3",
  - "walking_access_4",
  - "transport_access_1",
  - "transport_access_2",
  - "transport_access_3",
  - "transport_access_4",
- кол-во комнат
  - "offer_room_count" - количество комнат на продажу (если продается не вся квартира целиком
- общая
  - "total_square",
- жилая
  - "life_square",
- кухня
  - "kitchen_square",
- этаж
   - "storey",
- этажность
   - "storeys_count",
- материал дома
    - "walls_material_type_name"
- источник
    - "media_name"
- дата
   - "update_datetime"
- примечания
   - "note",
- фото ( При разговоре с Бикузиным я понял, что фото могут открываться с их серверов, чтоб не утерять наш) Но вам виднее
    - "photo_list" - содержит список идентификаторов фотографий; чтобы из идентификатора фотографии получить url фотографии надо префикс "https://images.baza-winner.ru/" и суффикс "_1024x768": "n595f7eb0081206088d9c43171fafc9da" => https://images.baza-winner.ru/n595f7eb0081206088d9c43171fafc9da_1024x768

## Поля загородка:
- адрес
- шоссе
- км от мкад
- площадь участка
- площадь дома
- категория земель
- материал дома
- этажей в доме
- газ, вода, эл- во, канализация
- примечания
- фото

## Так же еще такие поля:
- цена
  - "price",
- источник
  - "media_name"
- дата публикации
  - "pub_datetime"
- описание
  - "note",
- студия? 1 - да
  - "is_studio",
- апартаменты? 1 - да
  - "is_apartment",
- свободная планировка? 1 - да
  - "is_free_planning",

`export EDITOR=vim && crontab -e`

55 19 * * 4 /home/winnerdev/abc/prod/art_realtor_back for-analytics 2>/home/winnerdev/abc/prod/art_realtor_back/log

https://superuser.com/a/178591:
`nohup ./art_realtor_back for-analytics &disown`

`EDITOR=vim crontab -e`:

```crontab
# “At 13:30 on Wednesday.”
30 13 * * 3 /home/winnerdev/abc/prod/art_realtor_back/art_realtor_back -w /home/winnerdev/abc/prod/art_realtor_back for-site > /home/winnerdev/abc/prod/art_realtor_back/for-site.log 2>&1

# “At 14:00 on day-of-month 1 in every month.”
00 14 1 */1 * /home/winnerdev/abc/prod/art_realtor_back/art_realtor_back -w /home/winnerdev/abc/prod/art_realtor_back for-analytics > /home/winnerdev/abc/prod/art_realtor_back/for-analytics.log 2>&1
```
