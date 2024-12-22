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

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/robfig/cron/v3"
	"github.com/streadway/amqp"
	"github.com/twilio/twilio-go"
	openapi "github.com/twilio/twilio-go/rest/api/v2010"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoClient *mongo.Client
var rabbitmqConn *amqp.Connection
var rabbitmqChannel *amqp.Channel
var c *cron.Cron

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// initialize services
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
	StartCronJob()

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

	// shut down rabbitMQ
	closeRabbitMQ()

	fmt.Println("Server gracefully stopped")

}

// stores user preference, fetches initial weather data based on the preference
func handleUserPreferences(w http.ResponseWriter, r *http.Request) {
	var userPreferences shared.UserPreferences

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
func requestWeatherData(userPreferences shared.UserPreferences) {
	message := shared.Message{
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
				WeekendWeather  []shared.WeatherData   `json:"weekendWeather"`
				Recommendation  string                 `json:"recommendation"`
				UserPreferences shared.UserPreferences `json:"userPreferences"`
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
func processWeatherUpdate(weekendWeather []shared.WeatherData, recommendation string, userPreferences shared.UserPreferences) {
	for _, weatherData := range weekendWeather {
		log.Printf("Weather for %s: %+v", weatherData.DayOfWeek, weatherData)
	}
	log.Printf("Recommendation for user %s: %s", userPreferences.UserId, recommendation)
	sendWeatherRecommendation(recommendation, userPreferences)
}

// send recommendation via SMS through Twilio
func sendWeatherRecommendation(recommendation string, userPreferences shared.UserPreferences) {
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

/* ----------------------- CRON ------------------------*/
// start daily check job every day at 9AM
func StartCronJob() {
	c = cron.New()

	// schedule the job to run every day at 9AM
	c.AddFunc("0 9 * * *", func() {
		runDailyWeatherJob()
	})

	// start the cron scheduler
	c.Start()
	fmt.Println("Cron scheduler started. Running daily tasks at 9 AM.")
}

// graceful shutdown for cron job
func StopCronJob() {
	if c != nil {
		ctx := context.Background()
		c.Stop()
		fmt.Println("Cron scheduler stopped.")
		<-ctx.Done()
	}
}

// fetch weather and send sms for all users
func runDailyWeatherJob() {
	fmt.Println("Running daily weather job...")

	allUserPreferences := getAllUserPreferences()

	// iterate through each user preference, fetch and send
	for _, userPreferences := range allUserPreferences {
		err := requestDailyWeatherData(userPreferences)
		if err != nil {
			log.Println("Failed to fetch weather data:", err)
			return
		}
	}

	fmt.Println("Daily weather job completed")
}

// send weather request data to rabbitmq queue
func requestDailyWeatherData(userPreferences shared.UserPreferences) error {
	message := shared.Message{
		Action: "FetchWeather",
		Data:   userPreferences,
	}

	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal weather request: %v", err)
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
		return fmt.Errorf("failed to publish weather request: %v", err)
	}

	return nil

}

// gets list of UserPreferences type
func getAllUserPreferences() []shared.UserPreferences {
	var allUserPreferences []shared.UserPreferences

	collection := mongoClient.Database("weekendweatherdb").Collection("user_preferences")

	// get all docs
	cursor, err := collection.Find(context.Background(), bson.M{})
	if err != nil {
		log.Println("Failed to fetch user preferences:", err)
		return allUserPreferences
	}
	defer cursor.Close(context.Background())

	// iterate over documents returned and add to list of UserPreferences
	for cursor.Next(context.Background()) {
		var preference shared.UserPreferences
		err := cursor.Decode(&preference)
		if err != nil {
			log.Println("Failed to decode user preferences:", err)
		}
		allUserPreferences = append(allUserPreferences, preference)
	}

	return allUserPreferences
}
