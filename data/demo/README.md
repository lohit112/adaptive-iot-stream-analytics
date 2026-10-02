Demo datasets (columns: Timestamp, DeviceId, Sensor, Value). File row order = ARRIVAL order.
- sample_ooo.csv: 5 rows, deliberately out of order (spec example).
- sample_ooo_2k.csv: 2,400 rows, 3 devices x 2 sensors, bounded disorder in groups of 25 (seeded, 42).
  A few synthetic spikes were injected so the Isolation Forest has something to find. There is NO label column;
  do not report accuracy/precision/recall from this data.
