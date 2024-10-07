package weather

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
)

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

var ctx = context.Background()

// fetch weather from API and cache in redis
func FetchWeather(apiKey string, userPreferences UserPreferences, redisClient *redis.Client) ([]WeatherData, string, error) {
	zipCode := userPreferences.ZipCode
	redisKey := "weather:" + zipCode

	cachedWeather, err := redisClient.Get(ctx, redisKey).Result()

	if err == redis.Nil {
		log.Println("No cache. Fetching weather data from API.")

		// if no cache, fetch from api
		weekendWeather, recommendation, err := FetchWeatherFromAPI(apiKey, userPreferences)
		if err != nil {
			return nil, "", err
		}

		// cache the result for 1 hour
		weatherJSON, _ := json.Marshal(weekendWeather)
		err = redisClient.Set(ctx, redisKey, weatherJSON, time.Hour).Err()
		if err != nil {
			log.Println("Failed to cache weather data: ", err)
		}

		return weekendWeather, recommendation, nil

		// if other errors
	} else if err != nil {
		log.Println("Error fetching data from Redis: ", err)
		return nil, "", err
	}

	// otherwise cache hit
	log.Println("Cache hit. Returning cached data.")
	var weekendWeather []WeatherData
	json.Unmarshal([]byte(cachedWeather), &weekendWeather)

	recommendation := GenerateRecommendation(weekendWeather, userPreferences)
	return weekendWeather, recommendation, nil
}

// fetch from api
func FetchWeatherFromAPI(apiKey string, userPreferences UserPreferences) ([]WeatherData, string, error) {
	zipCode := userPreferences.ZipCode
	apiURL := fmt.Sprintf("https://api.weatherapi.com/v1/forecast.json?key=%s&q=%s&days=7", apiKey, zipCode)

	// set up structure of data fetched from api
	var forecastData struct {
		Forecast struct {
			ForecastDay []struct {
				Date string `json:"date"`
				Day  struct {
					MaxTemp   float64 `json:"maxtemp_c"`
					MinTemp   float64 `json:"mintemp_c"`
					Condition struct {
						Text string `json:"text"`
					} `json:"condition"`
				} `json:"day"`
			} `json:"forecastday"`
		} `json:"forecast"`
	}

	// make request
	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch weather data: %v", err)
	}
	defer resp.Body.Close()

	// error checking
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("received non-200 response code: %d", resp.StatusCode)
	}

	// parse to forecastData
	err = json.NewDecoder(resp.Body).Decode(&forecastData)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse weather data: %v", err)
	}

	// set up the return data
	var weekendWeather []WeatherData
	for _, day := range forecastData.Forecast.ForecastDay {
		// parse date into yyyy-mm-dd
		forecastDate, err := time.Parse("2006-01-02", day.Date)
		if err != nil {
			continue // ignore invalid days
		}
		// find the immediate weekend
		if forecastDate.Weekday() == time.Saturday {
			weekendWeather = append(weekendWeather, WeatherData{
				AvgTemp:   (day.Day.MaxTemp + day.Day.MinTemp) / 2,
				MinTemp:   (day.Day.MinTemp),
				MaxTemp:   (day.Day.MaxTemp),
				Condition: day.Day.Condition.Text,
				DayOfWeek: "Saturday",
			})
		}

		if forecastDate.Weekday() == time.Sunday {
			weekendWeather = append(weekendWeather, WeatherData{
				AvgTemp:   (day.Day.MaxTemp + day.Day.MinTemp) / 2,
				MinTemp:   (day.Day.MinTemp),
				MaxTemp:   (day.Day.MaxTemp),
				Condition: day.Day.Condition.Text,
				DayOfWeek: "Sunday",
			})
		}

		if len(weekendWeather) == 2 {
			break
		}
	}

	// generate recommendation
	recommendation := GenerateRecommendation(weekendWeather, userPreferences)

	return weekendWeather, recommendation, nil
}

// generate string recommendation based on the weather data and user preferences provided
func GenerateRecommendation(weekendWeather []WeatherData, userPreferences UserPreferences) string {

	// separate weather data
	var SatWeather, SunWeather WeatherData
	for _, weather := range weekendWeather {
		if weather.DayOfWeek == "Saturday" {
			SatWeather = weather
		}
		if weather.DayOfWeek == "Sunday" {
			SunWeather = weather
		}
	}

	SatBadTemp := SatWeather.MaxTemp > userPreferences.PreferredTemperatureMax || SatWeather.MinTemp < userPreferences.PreferredTemperatureMin
	SunBadTemp := SunWeather.MaxTemp > userPreferences.PreferredTemperatureMax || SunWeather.MinTemp < userPreferences.PreferredTemperatureMin

	SatBadCondition := !containsCondition(SatWeather.Condition, userPreferences.PreferredConditions)
	SunBadCondition := !containsCondition(SunWeather.Condition, userPreferences.PreferredConditions)

	SatSummary := fmt.Sprintf("Saturday - Min: %.2f°C, Max: %.2f°C, Avg: %.2f°C, Condition: %s", SatWeather.MinTemp, SatWeather.MaxTemp, SatWeather.AvgTemp, SatWeather.Condition)
	SunSummary := fmt.Sprintf("Sunday - Min: %.2f°C, Max: %.2f°C, Avg: %.2f°C, Condition: %s", SunWeather.MinTemp, SunWeather.MaxTemp, SunWeather.AvgTemp, SunWeather.Condition)

	// both days have bad temperature or conditions
	if (SatBadTemp || SatBadCondition) && (SunBadTemp || SunBadCondition) {
		return fmt.Sprintf(
			"Postpone your weekend plans - both days have unfavorable conditions:\n%s\n%s",
			SatSummary, SunSummary,
		)
	}

	// only Saturday has bad temperature or condition
	if SatBadTemp || SatBadCondition {
		return fmt.Sprintf(
			"Consider moving your weekend plans to Sunday. Here are the details:\n%s (unfavorable)\n%s (favorable)",
			SatSummary, SunSummary,
		)
	}

	// only Sunday has bad temperature or condition
	if SunBadTemp || SunBadCondition {
		return fmt.Sprintf(
			"Consider moving your weekend plans to Saturday. Here are the details:\n%s (favorable)\n%s (unfavorable)",
			SatSummary, SunSummary,
		)
	}

	// otherwise all good
	return fmt.Sprintf(
		"Go ahead with your weekend plans! Here are the details:\n%s\n%s",
		SatSummary, SunSummary,
	)
}

func containsCondition(curCondition string, preferredCondition []string) bool {
	for _, condition := range preferredCondition {
		if condition == curCondition {
			return true
		}
	}
	return false
}
