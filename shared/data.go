package shared

// structure to return sat and sun weather
type WeatherData struct {
	AvgTemp   float64 `json:"temp"`
	MinTemp   float64 `json:"min_temp"`
	MaxTemp   float64 `json:"max_temp"`
	Condition string  `json:"condition"`
	DayOfWeek string  `json:"day_of_week"`
}

// user specific settings
type UserPreferences struct {
	UserId                  string   `json:"userId"`
	ZipCode                 string   `json:"zipcode"`
	PreferredTemperatureMin float64  `json:"preferred_temperature_min"`
	PreferredTemperatureMax float64  `json:"preferred_temperature_max"`
	PreferredConditions     []string `json:"preferred_conditions"`
	PhoneNumber             string   `json:"phone_number"`
}

// message structure for RabbitMQ communication
type Message struct {
	Action string          `json:"action"`
	Data   UserPreferences `json:"data"`
}
