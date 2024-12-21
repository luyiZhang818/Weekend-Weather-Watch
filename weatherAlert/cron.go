package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"weekendWeather/weather"

	"github.com/robfig/cron/v3"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
)

var c *cron.Cron

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
func requestDailyWeatherData(userPreferences weather.UserPreferences) error {
	message := weather.Message{
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
func getAllUserPreferences() []weather.UserPreferences {
	var allUserPreferences []weather.UserPreferences

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
		var preference weather.UserPreferences
		err := cursor.Decode(&preference)
		if err != nil {
			log.Println("Failed to decode user preferences:", err)
		}
		allUserPreferences = append(allUserPreferences, preference)
	}

	return allUserPreferences
}
