package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"weekendWeather/shared"
	"weekendWeather/weather"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
	"github.com/twilio/twilio-go"
	openapi "github.com/twilio/twilio-go/rest/api/v2010"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoClient *mongo.Client
var rabbitmqConn *amqp.Connection
var rabbitmqChannel *amqp.Channel

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// initialize services
	shared.InitRedis()
	initMongoDB()
	initRabbitMQ()

	// set up router to handle POST requests to /preferences
	router := mux.NewRouter()
	router.HandleFunc("/preferences", handleUserPreferences).Methods("POST")

	// http server
	server := &http.Server{
		Addr:    ":8081",
		Handler: router,
	}

	// automated SMS delivery for users in database
	StartCronJob(shared.RedisClient)

	// listen for weather changes
	go consumeWeatherChanges()

	// start http server in goroutine on port 8081
	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not listen on %s: %v\n", server.Addr, err)
		}
	}()
	log.Printf("Server started on %s", server.Addr)

	// graceful shutdown setup
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("Shutting down server")

	// shut down cron job
	StopCronJob()

	// shut down http server (5s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = server.Shutdown(ctx)
	if err != nil {
		log.Fatal("Server forced to shutdown: ", err)
	}

	// shut down mongodb
	err = mongoClient.Disconnect(ctx)
	if err != nil {
		log.Fatal("MongoDB disconnection error:", err)
	}

	// shut down redis
	err = shared.RedisClient.Close()
	if err != nil {
		log.Fatal("Redis disconnection error:", err)
	}

	// shut down rabbitMQ
	closeRabbitMQ()

	fmt.Println("Server gracefully stopped")

}

// stores user preference, fetches initial weather data based on the preference
func handleUserPreferences(w http.ResponseWriter, r *http.Request) {
	var userPreferences weather.UserPreferences

	// parse
	err := json.NewDecoder(r.Body).Decode(&userPreferences)
	if err != nil {
		http.Error(w, "Invalid input", http.StatusBadRequest)
		return
	}

	// store in database with 5s cancel timeout
	collection := mongoClient.Database("weekendweatherdb").Collection("user_preferences")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = collection.InsertOne(ctx, userPreferences)
	if err != nil {
		http.Error(w, "Failed to save user preferences", http.StatusInternalServerError)
		return
	}

	// request initial weather data
	requestWeatherData(userPreferences)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("User preferences saved successfully"))
}

// creates a message requesting weather data for a userpreference and publishes to rabbitMQ queue
func requestWeatherData(userPreferences weather.UserPreferences) {
	message := weather.Message{
		Action: "FetchWeather",
		Data:   userPreferences,
	}

	body, err := json.Marshal(message)
	if err != nil {
		log.Printf("Failed to marshal weather request: %v", err)
		return
	}

	err = rabbitmqChannel.Publish(
		"",
		"weather_request_queue",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	if err != nil {
		log.Printf("Failed to publish weather request: %v", err)
	}
}

// listens for queue messages in queue and process the data
func consumeWeatherChanges() {
	messages, err := rabbitmqChannel.Consume(
		"weather_data_queue",
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

	forever := make(chan bool)

	go func() {
		for d := range messages {
			var updateMessage struct {
				WeekendWeather  []weather.WeatherData   `json:"weekendWeather"`
				Recommendation  string                  `json:"recommendation"`
				UserPreferences weather.UserPreferences `json:"userPreferences"`
			}
			err := json.Unmarshal(d.Body, &updateMessage)
			if err != nil {
				log.Printf("Error unmarshaling weather update: %s", err)
				continue
			}
			processWeatherUpdate(updateMessage.WeekendWeather, updateMessage.Recommendation, updateMessage.UserPreferences)
		}
	}()

	log.Printf("Waiting for weather change messages")
	<-forever
}

// process received weather data and send updates accordingly
func processWeatherUpdate(weekendWeather []weather.WeatherData, recommendation string, userPreferences weather.UserPreferences) {
	for _, weatherData := range weekendWeather {
		log.Printf("Weather for %s: %+v", weatherData.DayOfWeek, weatherData)
	}
	log.Printf("Recommendation for user %s: %s", userPreferences.UserId, recommendation)
	sendWeatherRecommendation(recommendation, userPreferences)
}

// send recommendation via SMS through Twilio
func sendWeatherRecommendation(recommendation string, userPreferences weather.UserPreferences) {
	// initialize Twilio client
	TwilioClient := twilio.NewRestClientWithParams(twilio.ClientParams{
		Username: os.Getenv("TWILIO_ACCOUNT_SID"),
		Password: os.Getenv("TWILIO_AUTH_TOKEN"),
	})

	// pass in parameters
	params := &openapi.CreateMessageParams{}
	params.SetTo(userPreferences.PhoneNumber)
	params.SetFrom(os.Getenv("TWILIO_PHONE_NUMBER"))
	params.SetBody(fmt.Sprintf("Weekend Weather Update:\n%s", recommendation))

	// send message
	resp, err := TwilioClient.Api.CreateMessage(params)
	if err != nil {
		log.Println("Failed to send SMS:", err)
		return
	} else {
		log.Printf("SMS sent successfully to %s, SID: %s\n", userPreferences.PhoneNumber, *resp.Sid)
	}

}

func initMongoDB() {
	uri := os.Getenv("MONGO_URI")
	clientOptions := options.Client().ApplyURI(uri)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	mongoClient, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal("Could not connect to MongoDB: ", err)
	}

	// check connection
	err = mongoClient.Ping(ctx, nil)
	if err != nil {
		log.Fatal("Could not connect to MongoDB:", err)
	}

	fmt.Println("Connected to MongoDB!")
}

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

func closeRabbitMQ() {
	if rabbitmqChannel != nil {
		rabbitmqChannel.Close()
	}
	if rabbitmqConn != nil {
		rabbitmqConn.Close()
	}
}
