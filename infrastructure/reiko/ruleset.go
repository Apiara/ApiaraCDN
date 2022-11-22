package reiko

import (
  "fmt"
  "time"
  "sync"
  "strings"
  "context"
  "github.com/go-redis/redis/v8"
)

const (
  //PrefixRulesRedisKey is the key that stores the set with all prefix rules in redis
  PrefixRulesRedisKey = "prefixes:valid"

  /* RuleSetRefreshDuration is the frequency at which the rule set will
  be re-read to check for updates */
  RuleSetRefreshDuration = time.Minute
)

// ContentRules allows modification of valid content rules as well as rule checking
type ContentRules interface {
  MatchesRule(cid string) (bool, error)
  SetRule(rule string) error
  DelRule(rule string) error
}

// PrefixContentRules implements ContentValidator using prefix rules
type PrefixContentRules struct {
  rdb *redis.Client
  ctx context.Context
  lastRead time.Time
  ruleSet []string
  mutex *sync.Mutex
}

/* NewPrefixContentRules creates a new PrefixContentRules with a
redis DB at addr as the storage unit */
func NewPrefixContentRules(addr string) *PrefixContentRules {
  client := redis.NewClient(&redis.Options{
    Addr: addr,
    Password: "",
    DB: 0,
  })

  return &PrefixContentRules{
    rdb: client,
    ctx: context.Background(),
    lastRead: time.Now().Add(-1*(RuleSetRefreshDuration * 2)),
    ruleSet: nil,
    mutex: &sync.Mutex{},
  }
}

// SetRule adds a prefix rule to the rule set
func (r *PrefixContentRules) SetRule(cidPrefix string) error {
  val, err := r.rdb.SAdd(r.ctx, PrefixRulesRedisKey, cidPrefix).Result()
  if err != nil {
    return fmt.Errorf("Failed to add prefix rule for %s: %w", cidPrefix, err)
  } else if val != 1 {
    return fmt.Errorf("Redis set addition returned with unsuccessful return value %d", val)
  }
  return nil
}

// DelRule removes a prefix rule from the rule set
func (r *PrefixContentRules) DelRule(cidPrefix string) error {
  val, err := r.rdb.SRem(r.ctx, PrefixRulesRedisKey, cidPrefix).Result()
  if err != nil {
    return fmt.Errorf("Failed to remove prefix rule for %s: %w", cidPrefix, err)
  } else if val != 1 {
    return fmt.Errorf("Redis set deletion failed with unsuccessful return value %d", val)
  }
  return nil
}

// getRules rate limits the amount of times we read from the redis DB
func (r *PrefixContentRules) getRules() ([]string, error) {
  if r.ruleSet != nil && time.Since(r.lastRead) < RuleSetRefreshDuration {
    return r.ruleSet, nil
  }

  val, err := r.rdb.SMembers(r.ctx, PrefixRulesRedisKey).Result()
  if err != nil {
    return nil, err
  }
  r.lastRead = time.Now()
  r.ruleSet = val
  return val, nil
}

// MatchesRule checks if has a prefix that matches the list of approved prefixes
func (r *PrefixContentRules) MatchesRule(cid string) (bool, error) {
  r.mutex.Lock()
  defer r.mutex.Unlock()

  rules, err := r.getRules()
  if err != nil {
    return false, err
  }

  for _, rule := range rules {
    if strings.HasPrefix(cid, rule) {
      return true, nil
    }
  }
  return false, nil
}
