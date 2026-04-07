package model

type DebeziumEvent struct {
	Before map[string]interface{} `json:"before"`
	After  struct {
		ID         interface{} `json:"id"`
		ExitPlaza  string      `json:"exit_plaza"`
		EntryPlaza string      `json:"entry_plaza"`
		MoneyValue float64     `json:"money_value"`
		UpdatedAt  interface{} `json:"updated_at"`
	} `json:"after"`
	Source struct {
		Version   string `json:"version"`
		Connector string `json:"connector"`
		Name      string `json:"name"`
		Table     string `json:"table"`
	} `json:"source"`
	Op   string `json:"op"`
	TsMs int64  `json:"ts_ms"`
}
