package goc

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type RedisConnection struct {
  Host string
  Port string
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

func TestConnection(client *redis.Client) (string, error) {
  pong, err := client.Ping(ctx).Result()
  return pong, err
}

func XReadGroup(xReadGroupArgs *redis.XReadGroupArgs, redis *redis.Client) *redis.XStreamSliceCmd {
  return redis.XReadGroup(ctx, xReadGroupArgs)
}

type ProcessMessage func(string)

// Reads as a consumer in consumer group from a
// given stream indefinitely
func XReadGroupBlocking(xReadGroupArgs *redis.XReadGroupArgs, redis *redis.Client, processMessage ProcessMessage) {
  if (XReadGroupArgs.Block != time.ParseDuration("0s")) {
    panic("XReadGroupBlocking should have indefinite block time (0s).")
  }
  for true {
    results, err := XReadGroup(XReadGroupArgs, client).Result()
    if (err != redis.Nil || err != nil) {
      panic(fmt.Sprintf("GOC::Redis Error %v", err))
    }
    if (len(results) > 0) {
      for _, result := range results {
        stream := results.Stream
        for _, message := range result.Messages {
          processMessage(message)
          updateIndex = -1
          currentStreams := xReadGroupArgs.Streams()
          for index, str := range currentStreams {
            if (str == stream) {
              updateIndex = index + 1
              break
            }
          }
          currentStreams[updateIndex] = stream
          xReadGroupArgs := &redis.XReadGroupArgs{
            Group: xReadGroupArgs.Group,
            Consumer: xReadGroupArgs.Consumer,
            Streams: currentStreams,
            Count: xReadGroupArgs.Count,
            Block: xReadGroupArgs.Block,
            NoAck: xReadGroupArgs.NoAck,
          }
        }
      }
    }
}
