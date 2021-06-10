package goc

import (
	"context"
	"fmt"
	"time"
	"github.com/go-redis/redis/v8"
  "errors"
)

var ctx = context.Background()

type RedisConnection struct {
  Host string
  Port uint16
  Password string
}

func (connection RedisConnection) NewClient() *redis.Client {
  return redis.NewClient(&redis.Options{
    Addr:     fmt.Sprintf("%v:%v", connection.Host, connection.Port),
    Password: connection.Password,
    DB:       0,
  })
}

func GetClient(connection RedisConnection) *redis.Client {
  return connection.NewClient()
}

func CheckConnection(client *redis.Client) (string, error) {
  pong, err := client.Ping(ctx).Result()
  return pong, err
}

type ProcessMessage func(redis.XMessage) string

// Reads as a consumer in consumer group from a
// given stream indefinitely
func XReadGroupBlocking(xReadGroupArgs *redis.XReadGroupArgs, client *redis.Client, processMessage ProcessMessage) error {
  indefiniteTime, _ := time.ParseDuration("0s")
  if (xReadGroupArgs.Block != indefiniteTime) {
    return errors.New("XReadGroupBlocking should have indefinite block time (0s).")
  }
  for true {
    results, err := client.XReadGroup(ctx, xReadGroupArgs).Result()
    if (err != redis.Nil && err != nil) {
      fmt.Println(err)
      return err
    }
    var lastProcessed string
    if (len(results) > 0) {
      for _, result := range results {
        if (len(result.Messages) == 0) { panic("No messages in stream!"); }
        for _, message := range result.Messages {
          lastProcessed = processMessage(message)
          if (lastProcessed == "ACKNOWLEDGED!") {
            _, err := client.XAck(ctx, result.Stream, xReadGroupArgs.Group, message.ID).Result()
            if (err != nil) { panic(err); }
          }
        }
      }
    }
    if (lastProcessed == "STOP EXECUTION!") {
      break;
    }
  }
  return nil
}

func XReadGroupBlockingCluster(xReadGroupArgs *redis.XReadGroupArgs, client *redis.ClusterClient, processMessage ProcessMessage) error {
  indefiniteTime, _ := time.ParseDuration("0s")
  if (xReadGroupArgs.Block != indefiniteTime) {
    return errors.New("XReadGroupBlocking should have indefinite block time (0s).")
  }
  for true {
    results, err := client.XReadGroup(ctx, xReadGroupArgs).Result()
    if (err != redis.Nil && err != nil) {
      fmt.Println(err)
      return err
    }
    var lastProcessed string
    if (len(results) > 0) {
      for _, result := range results {
        if (len(result.Messages) == 0) { panic("No messages in stream!"); }
        for _, message := range result.Messages {
          lastProcessed = processMessage(message)
          if (lastProcessed == "ACKNOWLEDGED!") {
            _, err := client.XAck(ctx, result.Stream, xReadGroupArgs.Group, message.ID).Result()
            if (err != nil) { panic(err); }
          }
        }
      }
    }
    if (lastProcessed == "STOP EXECUTION!") {
      break;
    }
  }
  return nil
}

func InsertInStream(client *redis.Client, stream string, id string, values []string, maxLen int64, maxLenApprox int64) {
  xAddArgs := redis.XAddArgs{}
  xAddArgs.Stream = stream
  xAddArgs.ID = id
  xAddArgs.Values = values
  if (maxLen != 0) {
    xAddArgs.MaxLen = maxLen
  }
  if (maxLenApprox != 0) {
    xAddArgs.MaxLenApprox = maxLenApprox
  }
  client.XAdd(ctx, &xAddArgs)
}
