package deus

import (
  "context"
  "github.com/go-redis/redis/v8"
)

type ContentValidator interface {
  MatchesPrefixRule(cid string) bool
  SetPrefixRule(cidPrefix string) error
}

type RedisContentValidator struct {
  rdb *redis.Client
  ctx context.Context
}

func NewRedisContentValidator(addr string) *RedisContentValidator {
  client := redis.NewClient(&redis.Options{
    Addr: addr,
    Password: "",
    DB: 0,
  })

  return &RedisContentValidator{
    rdb: client,
    ctx: context.Background(),
  }
}

func generatePrefixRuleKey(cidPrefix string) string {
  return "PREFIX|" + cidPrefix
}

func unpackPrefixRuleKey(key string) string {
  _, cidPrefix, _ := strings.Cut(key, "|")
  return cidPrefix
}

func (v *RedisContentValidator) SetPrefixRule(cidPrefix string) error {
  key := generatePrefixRuleKey(cidPrefix)
  err := r.rdb.Set(r.ctx, key, "", 0).Err()
  if err != nil {
    return fmt.Errorf("")
  }
}
