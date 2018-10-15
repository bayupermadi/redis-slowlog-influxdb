import redis
import datetime
from influxdb import InfluxDBClient

# Redis Target
host_target     = "192.168.1.202"
port_target     = 6379
num_target      = 10

# InfluxDB
influx_host     = "192.168.1.203"
influx_port     = 8086
influx_db       = "monitoring"

pool = redis.ConnectionPool(host=host_target, port=port_target, db=0)

r = redis.Redis(connection_pool=pool)

logs = r.slowlog_get(num_target)

for entry in logs:
    timestamp   = datetime.datetime.fromtimestamp(int(entry['start_time']))
    duration    = round(int(entry['duration'])) / 1000 / 1000
    command     = unicode(entry['command'], errors='ignore')

    
    data_monitor = [
        {
            "measurement": "redis_slowlog",
            "tags": {
                "host": host_target
            },
            "time": timestamp,
            "fields": {
                "command": command,
                "duration": duration
            }
        }
    ]

    client = InfluxDBClient(influx_host, influx_port, 'root', 'root', influx_db)
    client.create_database(influx_db)
    client.create_retention_policy('data_retention', '30d', '1', influx_db, default=True)
    client.write_points(data_monitor)
