package goc

import (
	"reflect"
	"testing"
  "time"

	"github.com/go-redis/redis/v8"
)

var port uint16 = 6379
var connection = RedisConnection{
  Host:"127.0.0.1",
  Port:port,
  Password:"",
}

func TestGetClient(*testing.T) {
  client := GetClient(connection)
  if (reflect.TypeOf(client).String() != "*redis.Client") {
    panic("GetClient has not returned a redis client reference")
  }
}

func TestCheckConnection(*testing.T) {
  pong, err := CheckConnection(GetClient(connection))
  if (err != nil) {
    panic(err)
  }
  if (reflect.TypeOf(pong).String() != "string") {
    panic("Pong not recieved")
  }
}

func TestXReadGroupBlockingIndefinitely(*testing.T) {
  definiteBlocking, _ := time.ParseDuration("1s")
  xReadGroupArgs := &redis.XReadGroupArgs{
    Streams:[]string{"stream1",">"},
    Group:"pi-zero-arkham-spyder-messages",
    Count:0,
    Block:definiteBlocking,
    NoAck:false,
  }
  processMessage := func(message redis.XMessage) string { return "PROCESSED!"; }
  err := XReadGroupBlocking(xReadGroupArgs, GetClient(connection), processMessage)
  if (err == nil) {
    panic("Error check for indefinite blocking failed")
  }
}

func TestXReadGroupBlocking(*testing.T) {
  // insert dummy data
  client := GetClient(connection)
  client.Del(ctx, "stream-1")
  _, err := client.XAdd(ctx, &redis.XAddArgs{
    Stream:"stream-1",
    ID:"*",
    Values:[]string{"message","first-message"},
  }).Result()
  if (err != nil) { panic(err); }
  _, err = client.XAdd(ctx, &redis.XAddArgs{
    Stream:"stream-1",
    ID:"*",
    Values:[]string{"message","second-message"},
  }).Result()
  if (err != nil) { panic(err); }
  _, err = client.XGroupCreate(ctx, "stream-1", "mygroup", "0").Result()
  if (err != nil) { panic(err); }

  processedTimes := 0
  processMessage := func(message redis.XMessage) string {
    processedTimes += 1;
    if (processedTimes == 1 && message.Values["message"] != "first-message") {
      panic("First message not received correctly.")
    }
    if (processedTimes == 2 && message.Values["message"] != "second-message") {
      panic("Second message not received correctly.")
    }
    if (processedTimes == 2) { return "STOP EXECUTION!"; }
    return "ACKNOWLEDGED!"
  }
  indefiniteBlocking, _ := time.ParseDuration("0s")
  xReadGroupArgs := &redis.XReadGroupArgs{
    Streams:[]string{"stream-1",">"},
    Group:"mygroup",
    Consumer:"myconsumer",
    Count:1,
    Block:indefiniteBlocking,
    NoAck:false,
  }
  err = XReadGroupBlocking(xReadGroupArgs, client, processMessage)
  if (err != nil) {
    panic(err)
  }
  _, err = client.XGroupDestroy(ctx, "stream-1", "mygroup").Result()
  if (err != nil) { panic(err); }
}
