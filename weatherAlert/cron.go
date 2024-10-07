package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"weekendWeather/weather"

	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson"
)

var c *cron.Cron

// start daily check job every day at 9AM
func StartCronJob(redisClient *redis.Client) {
	c := cron.New()

	// schedule the job to run every day at 9AM
	c.AddFunc("0 9 * * *", func() {
		runDailyWeatherJob(redisClient)
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
func runDailyWeatherJob(redisClient *redis.Client) {
	fmt.Println("Running daily weather job...")

	allUserPreferences := getAllUserPreferences()

	// iterate through each user preference, fetch and send
	for _, userPreferences := range allUserPreferences {
		apiKey := os.Getenv("WEATHER_API_KEY")
		_, recommendation, err := weather.FetchWeather(apiKey, userPreferences, redisClient)
		if err != nil {
			log.Println("Failed to fetch weather data:", err)
			continue
		}
		sendWeatherRecommendation(recommendation, userPreferences)
	}
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
