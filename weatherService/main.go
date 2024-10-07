package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"weekendWeather/shared"
	"weekendWeather/weather"

	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

type Message struct {
	Action string                  `json:"action"`
	Data   weather.UserPreferences `json:"data"`
}

type OutgoingMessage struct {
	WeekendWeather  []weather.WeatherData   `json:"weekendWeather"`
	Recommendation  string                  `json:"recommendation"`
	UserPreferences weather.UserPreferences `json:"userPreferences"`
}

var rabbitmqConn *amqp.Connection
var rabbitmqChannel *amqp.Channel

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// initialize services
	initRabbitMQ()
	shared.InitRedis()

	go consumeWeatherChanges()

	forever := make(chan bool)
	<-forever

	// shut down redis
	err = shared.RedisClient.Close()
	if err != nil {
		log.Fatal("Redis disconnection error:", err)
	}

	// shut down rabbitMQ
	closeRabbitMQ()
}

// listen for requests and fetch weather with userPreferences
func consumeWeatherChanges() {
	messages, err := rabbitmqChannel.Consume(
		"weather_request_queue",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %s", err)
	}

	for d := range messages {
		var message Message
		err := json.Unmarshal(d.Body, &message)
		if err != nil {
			log.Printf("Error unmarshaling message: %s", err)
			continue
		}

		if message.Action == "FetchWeather" {
			go fetchAndProcessWeather(message.Data)
		}
	}
}

// fetch data from api with redis caching
func fetchAndProcessWeather(userPreferences weather.UserPreferences) {
	apiKey := os.Getenv("WEATHER_API_KEY")
	weekendWeather, recommendation, err := weather.FetchWeather(apiKey, userPreferences, shared.RedisClient)
	if err != nil {
		log.Printf("Failed to fetch weather data for user: %v", err)
		return
	}

	sendWeatherUpdate(weekendWeather, recommendation, userPreferences)
}

// send message back - sending weekendweather, recommendation and userpreferences
func sendWeatherUpdate(weekendWeather []weather.WeatherData, recommendation string, userPreferences weather.UserPreferences) {
	updateMessage := OutgoingMessage{
		WeekendWeather:  weekendWeather,
		Recommendation:  recommendation,
		UserPreferences: userPreferences,
	}

	body, err := json.Marshal(updateMessage)
	if err != nil {
		log.Printf("Error marshaling weather update: %s", err)
		return
	}

	err = rabbitmqChannel.Publish(
		"",
		"weather_data_queue",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	if err != nil {
		log.Printf("Failed to publish weather update: %s", err)
	}
}

// initialize rabitMQ
func initRabbitMQ() {
	var err error
	rabbitmqConn, err = amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}

	rabbitmqChannel, err = rabbitmqConn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
	}

	_, err = rabbitmqChannel.QueueDeclare(
		"weather_data_queue",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %s", err)
	}

	fmt.Println("Connected to RabbitMQ!")
}

// shut down rabbitMQ
func closeRabbitMQ() {
	if rabbitmqChannel != nil {
		rabbitmqChannel.Close()
	}
	if rabbitmqConn != nil {
		rabbitmqConn.Close()
	}
}
