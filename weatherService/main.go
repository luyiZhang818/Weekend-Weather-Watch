package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"weatherService/fetch"
	"weekendWeather/shared"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

type Message struct {
	Action string                 `json:"action"`
	Data   shared.UserPreferences `json:"data"`
}

type OutgoingMessage struct {
	WeekendWeather  []shared.WeatherData   `json:"weekendWeather"`
	Recommendation  string                 `json:"recommendation"`
	UserPreferences shared.UserPreferences `json:"userPreferences"`
}

var rabbitmqConn *amqp.Connection
var rabbitmqChannel *amqp.Channel
var RedisClient *redis.Client

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// initialize services
	initRabbitMQ()
	InitRedis()

	go consumeWeatherChanges()

	forever := make(chan bool)
	<-forever

	// shut down redis
	err = RedisClient.Close()
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
func fetchAndProcessWeather(userPreferences shared.UserPreferences) {
	apiKey := os.Getenv("WEATHER_API_KEY")
	weekendWeather, recommendation, err := fetch.FetchWeather(apiKey, userPreferences, RedisClient)
	if err != nil {
		log.Printf("Failed to fetch weather data for user: %v", err)
		return
	}

	sendWeatherUpdate(weekendWeather, recommendation, userPreferences)
}

// send message back - sending weekendweather, recommendation and userpreferences
func sendWeatherUpdate(weekendWeather []shared.WeatherData, recommendation string, userPreferences shared.UserPreferences) {
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

// initialize Redis
func InitRedis() {
	redisAddr := os.Getenv("REDIS_ADDR")
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})

	ctx := context.Background()

	// check connection
	_, err := RedisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Could not connect to Redis:", err)
	}

	fmt.Println("Connected to Redis!")
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
		"weather_request_queue",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare weather_request_queue: %s", err)
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
