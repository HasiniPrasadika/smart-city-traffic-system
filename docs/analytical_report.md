# Daily Analytical Report

## Objective

The analytical layer summarizes traffic activity from four Colombo junctions and identifies the most congested hour for each junction. The report is designed for traffic management teams that need to decide where police intervention or signal timing review may be required.

## Data Used

The batch report uses `data/traffic_data.csv`, which contains traffic readings for:

- J001: Borella Junction
- J002: Town Hall Junction
- J003: Maradana Junction
- J004: Nugegoda Junction

Each record contains `sensor_id`, `junction_name`, `timestamp`, `vehicle_count`, and `avg_speed`.

## Method

The report converts each event timestamp into an hour of day, then groups records by sensor, junction, and hour. For each group, it calculates:

- Total vehicle count
- Average speed
- Congestion index

The congestion index is calculated as:

```text
Congestion Index = Total Vehicle Count / Average Speed
```

A higher congestion index means a larger number of vehicles are moving at a lower average speed. This indicates heavier congestion.

## Results

The generated report identifies these peak congestion periods:

| Sensor | Junction | Peak Hour | Interpretation |
| --- | --- | ---: | --- |
| J001 | Borella Junction | 08:00 | Morning rush-hour congestion |
| J002 | Town Hall Junction | 09:00 | Heavy central-city flow after morning peak begins |
| J003 | Maradana Junction | 17:00 | Evening return-trip congestion |
| J004 | Nugegoda Junction | 18:00 | Evening suburban traffic congestion |

The report recommends traffic police intervention when the average speed is below 10 km/h or when the congestion index exceeds 15. In the sample data, all four junctions meet the intervention condition during their peak hour.

## Chart Interpretation

The generated chart `reports/traffic_volume_chart.png` shows total traffic volume against hour of day. It helps identify demand peaks across the entire monitored area. Morning traffic is visible around 08:00-09:00, while the strongest evening movement appears around 17:00-18:00.

## Operational Recommendation

Traffic officers should prioritize Borella Junction and Town Hall Junction in the morning, then shift attention to Maradana Junction and Nugegoda Junction during the evening. The system should be extended with more historical data so weekday/weekend patterns, special-event congestion, and school-term effects can be separated.

