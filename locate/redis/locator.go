package redis

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/tls"
	"github.com/dobyte/due/v2/encoding/json"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/locate"
	"github.com/dobyte/due/v2/log"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const (
	userGateKey     = "%s:locate:user:%d:gate"     // string
	userNodeKey     = "%s:locate:user:%d:node"     // hash
	clusterEventKey = "%s:locate:cluster:%s:event" // channel
)

const name = "redis"

var _ locate.Locator = &Locator{}

type Locator struct {
	err              error
	opts             *options
	builtin          bool
	ctx              context.Context
	cancel           context.CancelFunc
	sfg              singleflight.Group
	watchers         sync.Map
	unbindGateScript *redis.Script
	unbindNodeScript *redis.Script
}

func NewLocator(opts ...Option) *Locator {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	l := &Locator{}

	defer func() {
		if l.err == nil {
			l.opts = o
			l.ctx, l.cancel = context.WithCancel(o.ctx)
			l.unbindGateScript = redis.NewScript(unbindGateScript)
			l.unbindNodeScript = redis.NewScript(unbindNodeScript)
		}
	}()

	if o.client == nil {
		options := &redis.UniversalOptions{
			Addrs:      o.addrs,
			DB:         o.db,
			Username:   o.username,
			Password:   o.password,
			MaxRetries: o.maxRetries,
		}

		if o.certFile != "" && o.keyFile != "" && o.caFile != "" {
			if options.TLSConfig, l.err = tls.MakeRedisTLSConfig(o.certFile, o.keyFile, o.caFile); l.err != nil {
				return l
			}
		}

		o.client, l.builtin = redis.NewUniversalClient(options), true
	}

	return l
}

// Name 获取定位器组件名
func (l *Locator) Name() string {
	return name
}

// LocateGate 定位用户所在网关
func (l *Locator) LocateGate(ctx context.Context, uid int64) (string, error) {
	if l.err != nil {
		return "", l.err
	}

	key := fmt.Sprintf(userGateKey, l.opts.prefix, uid)

	val, err, _ := l.sfg.Do(key, func() (any, error) {
		val, err := l.opts.client.Get(ctx, key).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return "", err
		}

		return val, nil
	})
	if err != nil {
		return "", err
	}

	return val.(string), nil
}

// deduplicateUIDs 过滤非正数并去重 UID 列表
func deduplicateUIDs(uids []int64) []int64 {
	if len(uids) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(uids))
	out := make([]int64, 0, len(uids))
	for _, uid := range uids {
		if uid <= 0 {
			continue
		}
		if _, dup := seen[uid]; dup {
			continue
		}
		seen[uid] = struct{}{}
		out = append(out, uid)
	}
	return out
}

// LocateGates 批量定位用户所在网关（兼容单机与 Redis Cluster 单次 Pipeline 合并查询）
func (l *Locator) LocateGates(ctx context.Context, uids []int64) (map[int64]string, error) {
	if l.err != nil {
		return nil, l.err
	}

	unique := deduplicateUIDs(uids)
	res := make(map[int64]string, len(unique))
	if len(unique) == 0 {
		return res, nil
	}
	if len(unique) == 1 {
		gid, err := l.LocateGate(ctx, unique[0])
		if err != nil {
			return nil, err
		}
		if gid != "" {
			res[unique[0]] = gid
		}
		return res, nil
	}

	pipe := l.opts.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(unique))
	for i, uid := range unique {
		cmds[i] = pipe.Get(ctx, fmt.Sprintf(userGateKey, l.opts.prefix, uid))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for i, cmd := range cmds {
		if gid, err := cmd.Result(); err == nil && gid != "" {
			res[unique[i]] = gid
		}
	}
	return res, nil
}

// LocateNode 定位用户所在节点
func (l *Locator) LocateNode(ctx context.Context, uid int64, name string) (string, error) {
	if l.err != nil {
		return "", l.err
	}

	key := fmt.Sprintf(userNodeKey, l.opts.prefix, uid)

	val, err, _ := l.sfg.Do(key+name, func() (any, error) {
		val, err := l.opts.client.HGet(ctx, key, name).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return "", err
		}

		return val, nil
	})
	if err != nil {
		return "", err
	}

	return val.(string), nil
}

// LocateNodes 定位用户所在节点列表
func (l *Locator) LocateNodes(ctx context.Context, uid int64) (map[string]string, error) {
	if l.err != nil {
		return nil, l.err
	}

	key := fmt.Sprintf(userNodeKey, l.opts.prefix, uid)

	val, err, _ := l.sfg.Do(key, func() (any, error) {
		val, err := l.opts.client.HGetAll(ctx, key).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}

		return val, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(map[string]string), nil
}

// BatchLocateNode 批量定位用户所在指定名称节点
func (l *Locator) BatchLocateNode(ctx context.Context, uids []int64, name string) (map[int64]string, error) {
	if l.err != nil {
		return nil, l.err
	}

	unique := deduplicateUIDs(uids)
	res := make(map[int64]string, len(unique))
	if len(unique) == 0 || name == "" {
		return res, nil
	}
	if len(unique) == 1 {
		nid, err := l.LocateNode(ctx, unique[0], name)
		if err != nil {
			return nil, err
		}
		if nid != "" {
			res[unique[0]] = nid
		}
		return res, nil
	}

	pipe := l.opts.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(unique))
	for i, uid := range unique {
		cmds[i] = pipe.HGet(ctx, fmt.Sprintf(userNodeKey, l.opts.prefix, uid), name)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for i, cmd := range cmds {
		if nid, err := cmd.Result(); err == nil && nid != "" {
			res[unique[i]] = nid
		}
	}
	return res, nil
}

// BatchLocateNodes 批量定位用户所在节点列表
func (l *Locator) BatchLocateNodes(ctx context.Context, uids []int64) (map[int64]map[string]string, error) {
	if l.err != nil {
		return nil, l.err
	}

	unique := deduplicateUIDs(uids)
	res := make(map[int64]map[string]string, len(unique))
	if len(unique) == 0 {
		return res, nil
	}
	if len(unique) == 1 {
		nodes, err := l.LocateNodes(ctx, unique[0])
		if err != nil {
			return nil, err
		}
		if len(nodes) > 0 {
			res[unique[0]] = nodes
		}
		return res, nil
	}

	pipe := l.opts.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(unique))
	for i, uid := range unique {
		cmds[i] = pipe.HGetAll(ctx, fmt.Sprintf(userNodeKey, l.opts.prefix, uid))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for i, cmd := range cmds {
		if nodes, err := cmd.Result(); err == nil && len(nodes) > 0 {
			res[unique[i]] = nodes
		}
	}
	return res, nil
}

// BindGate 绑定网关
func (l *Locator) BindGate(ctx context.Context, uid int64, gid string) error {
	if l.err != nil {
		return l.err
	}

	key := fmt.Sprintf(userGateKey, l.opts.prefix, uid)

	if err := l.opts.client.Set(ctx, key, gid, redis.KeepTTL).Err(); err != nil {
		return err
	}

	if err := l.broadcast(ctx, locate.BindGate, uid, gid); err != nil {
		log.Errorf("location event broadcast failed: %v", err)
	}

	return nil
}

// BindNode 绑定节点
func (l *Locator) BindNode(ctx context.Context, uid int64, name, nid string) error {
	if l.err != nil {
		return l.err
	}

	key := fmt.Sprintf(userNodeKey, l.opts.prefix, uid)

	if err := l.opts.client.HSet(ctx, key, name, nid).Err(); err != nil {
		return err
	}

	if err := l.broadcast(ctx, locate.BindNode, uid, nid, name); err != nil {
		log.Errorf("location event broadcast failed: %v", err)
	}

	return nil
}

// UnbindGate 解绑网关
func (l *Locator) UnbindGate(ctx context.Context, uid int64, gid string) error {
	if l.err != nil {
		return l.err
	}

	key := fmt.Sprintf(userGateKey, l.opts.prefix, uid)

	rst, err := l.unbindGateScript.Run(ctx, l.opts.client, []string{key}, gid).StringSlice()
	if err != nil {
		return err
	}

	if rst[0] == "OK" {
		if err = l.broadcast(ctx, locate.UnbindGate, uid, gid); err != nil {
			log.Errorf("location event broadcast failed: %v", err)
		}
	}

	return nil
}

// UnbindNode 解绑节点
func (l *Locator) UnbindNode(ctx context.Context, uid int64, name, nid string) error {
	if l.err != nil {
		return l.err
	}

	key := fmt.Sprintf(userNodeKey, l.opts.prefix, uid)

	rst, err := l.unbindNodeScript.Run(ctx, l.opts.client, []string{key}, name, nid).StringSlice()
	if err != nil {
		return err
	}

	if rst[0] == "OK" {
		if err = l.broadcast(ctx, locate.UnbindNode, uid, nid, name); err != nil {
			log.Errorf("location event broadcast failed: %v", err)
		}
	}

	return nil
}

// 广播事件
func (l *Locator) broadcast(ctx context.Context, typ locate.EventType, uid int64, insID string, insName ...string) error {
	evt := &locate.Event{UID: uid, Type: typ, InsID: insID}

	switch typ {
	case locate.BindGate, locate.UnbindGate:
		evt.InsKind = cluster.Gate.String()
	case locate.BindNode, locate.UnbindNode:
		evt.InsKind = cluster.Node.String()
	}

	if len(insName) > 0 {
		evt.InsName = insName[0]
	}

	msg, err := marshal(evt)
	if err != nil {
		return err
	}

	return l.opts.client.Publish(ctx, fmt.Sprintf(clusterEventKey, l.opts.prefix, evt.InsKind), msg).Err()
}

func (l *Locator) toUniqueKey(kinds ...string) string {
	slices.Sort(kinds)

	return strings.Join(kinds, "&")
}

// Watch 监听用户定位变化
func (l *Locator) Watch(ctx context.Context, kinds ...string) (locate.Watcher, error) {
	if l.err != nil {
		return nil, l.err
	}

	key := l.toUniqueKey(kinds...)

	v, ok := l.watchers.Load(key)
	if ok {
		return v.(*watcherMgr).fork(), nil
	}

	w, err := newWatcherMgr(ctx, l, key, kinds...)
	if err != nil {
		return nil, err
	}

	l.watchers.Store(key, w)

	return w.fork(), nil
}

func marshal(event *locate.Event) (string, error) {
	buf, err := json.Marshal(event)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func unmarshal(data []byte) (*locate.Event, error) {
	evt := &locate.Event{}

	if err := json.Unmarshal(data, evt); err != nil {
		return nil, err
	}

	return evt, nil
}
