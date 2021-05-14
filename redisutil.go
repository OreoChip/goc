package goc

import (
	"context"
	"fmt"
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
