package simulator

type CycleStats struct {
	Time          int64   `parquet:"name=time, type=INT64"`
	Queue         string  `parquet:"name=queue, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Pool          string  `parquet:"name=pool, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PriorityClass string  `parquet:"name=priority_class, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Cpu           float64 `parquet:"name=cpu, type=DOUBLE"`
	Memory        float64 `parquet:"name=memory, type=DOUBLE"`
	Gpu           float64 `parquet:"name=gpu, type=DOUBLE"`
}
