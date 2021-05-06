"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str



app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

topic = app.topic("cta.stations", value_type=Station)
out_topic = app.topic("stations.table", partitions=1, value_type=TransformedStation)

table = app.Table(
   "TransformedStation",
   default=int,
   partitions=1,
   changelog_topic=out_topic,
)

@app.agent(topic)
async def setLine(station_stream):
    async for station_event in station_stream:
        if station_event.red == True:
            line_color = 'red'
        elif station_event.blue == True:
            line_color = 'blue'
        elif station_event.green == True:
            line_color = 'green'
        station_event_processed = TransformedStation(
            station_id = station_event.station_id,
            station_name = station_event.station_name,
            order = station_event.order,
            line = line_color
        )
        table[station_event.station_id] = station_event_processed
        await out_topic.send(key= str(station_event.station_id), value=station_event_processed)


if __name__ == "__main__":
    app.main()
