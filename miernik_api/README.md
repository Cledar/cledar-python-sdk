TODO:
* Improve error handling
* Connect to db (sqlalchemy?)
* Add tests
* Dockerize

To create a virtual environment:
```
conda env create -f environment.yml
conda activate miernik-api
```

To execute:
* `auth/login`
```
curl -X 'POST' \
  'http://127.0.0.1:8000/auth/login' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "app_id": "xxxx",
  "app_secret": "xxxx"
}'
```
* `device/status`
```
curl -X 'POST' \
  -H 'Authorization: bearer <token>' \
  'http://127.0.0.1:8000/device/status' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "status": {
    "battery_percentage": 0,
    "geo_lat": 0,
    "geo_lng": 0,
    "imei": "string",
    "connection_type": "string",
    "app_version": "string",
    "os_version": "string",
    "imsi": "string",
    "activity_type": "string",
    "startup_time": 0
  },
  "audio_hashes": [
    {
      "audio_hash": "string",
      "duration_ms": 0,
      "start_ms": 0
    }
  ]
}'
```
etc. as in http://127.0.0.1:8000/docs#/