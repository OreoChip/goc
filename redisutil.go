package goc

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type RedisConnection struct {
  host string
  port string
  password string
}

func (connection RedisConnection) NewClient() *redis.Client {
  return redis.NewClient(&redis.Options{
    Addr:     fmt.Sprintf("%v:%v", connection.host, connection.port),
    Password: connection.password,
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
