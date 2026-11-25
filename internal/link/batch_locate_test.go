package link

import (
	"context"
	"testing"

	"github.com/dobyte/due/v2/locate"
)

type stubBatchLocator struct {
	gateCalls      int
	lastGateUIDs   []int64
	gateStore      map[int64]string
	nodeCalls      int
	lastNodeUIDs   []int64
	nodeStore      map[int64]map[string]string
	nodesBatchCall int
}

func (s *stubBatchLocator) Name() string                                          { return "stub" }
func (s *stubBatchLocator) Watch(context.Context, ...string) (locate.Watcher, error) { return nil, nil }
func (s *stubBatchLocator) BindGate(context.Context, int64, string) error         { return nil }
func (s *stubBatchLocator) BindNode(context.Context, int64, string, string) error { return nil }
func (s *stubBatchLocator) UnbindGate(context.Context, int64, string) error       { return nil }
func (s *stubBatchLocator) UnbindNode(context.Context, int64, string, string) error {
	return nil
}
func (s *stubBatchLocator) LocateGate(_ context.Context, uid int64) (string, error) {
	return s.gateStore[uid], nil
}
func (s *stubBatchLocator) LocateGates(_ context.Context, uids []int64) (map[int64]string, error) {
	s.gateCalls++
	s.lastGateUIDs = append([]int64(nil), uids...)
	res := make(map[int64]string, len(uids))
	for _, uid := range uids {
		if gid := s.gateStore[uid]; gid != "" {
			res[uid] = gid
		}
	}
	return res, nil
}
func (s *stubBatchLocator) LocateNode(_ context.Context, uid int64, name string) (string, error) {
	if m := s.nodeStore[uid]; m != nil {
		return m[name], nil
	}
	return "", nil
}
func (s *stubBatchLocator) BatchLocateNode(_ context.Context, uids []int64, name string) (map[int64]string, error) {
	s.nodeCalls++
	s.lastNodeUIDs = append([]int64(nil), uids...)
	res := make(map[int64]string, len(uids))
	for _, uid := range uids {
		if m := s.nodeStore[uid]; m != nil && m[name] != "" {
			res[uid] = m[name]
		}
	}
	return res, nil
}
func (s *stubBatchLocator) LocateNodes(_ context.Context, uid int64) (map[string]string, error) {
	return s.nodeStore[uid], nil
}
func (s *stubBatchLocator) BatchLocateNodes(_ context.Context, uids []int64) (map[int64]map[string]string, error) {
	s.nodesBatchCall++
	res := make(map[int64]map[string]string, len(uids))
	for _, uid := range uids {
		if m := s.nodeStore[uid]; len(m) > 0 {
			res[uid] = m
		}
	}
	return res, nil
}

func TestGateLinker_LocateGates_L1AndBatchBackfill(t *testing.T) {
	stub := &stubBatchLocator{
		gateStore: map[int64]string{
			1002: "gate-2",
		},
	}
	linker := NewGateLinker(context.Background(), &Options{Locator: stub})
	linker.sources.Store(int64(1001), "gate-1") // L1 已缓存

	// 传入: 1001(L1命中), 1002(Redis在线), 1003(离线), 1002(重复), -1(非法)
	got, err := linker.LocateGates(context.Background(), []int64{1001, 1002, 1003, 1002, -1})
	if err != nil {
		t.Fatalf("LocateGates failed: %v", err)
	}
	if len(got) != 2 || got[1001] != "gate-1" || got[1002] != "gate-2" {
		t.Fatalf("unexpected LocateGates result: %+v", got)
	}
	if stub.gateCalls != 1 || len(stub.lastGateUIDs) != 2 || stub.lastGateUIDs[0] != 1002 || stub.lastGateUIDs[1] != 1003 {
		t.Fatalf("expected Locator.LocateGates called once with only missUIDs [1002 1003], got calls=%d uids=%v", stub.gateCalls, stub.lastGateUIDs)
	}

	// 验证 1002 已回填至 L1 缓存：再次仅查 [1001, 1002] 应 0 次穿透底层 Locator
	got2, err := linker.LocateGates(context.Background(), []int64{1001, 1002})
	if err != nil || len(got2) != 2 || stub.gateCalls != 1 {
		t.Fatalf("expected second query for online cached UIDs to hit L1 with 0 new Locator calls, got calls=%d res=%+v", stub.gateCalls, got2)
	}
}

func TestNodeLinker_BatchLocateNodeAndNodes(t *testing.T) {
	stub := &stubBatchLocator{
		nodeStore: map[int64]map[string]string{
			2002: {"monopoly01": "node-b"},
		},
	}
	linker := NewNodeLinker(context.Background(), &Options{Locator: stub})
	linker.doStoreSource(2001, "monopoly01", "node-a")

	got, err := linker.BatchLocateNode(context.Background(), []int64{2001, 2002, 2003, 2002}, "monopoly01")
	if err != nil {
		t.Fatalf("BatchLocateNode failed: %v", err)
	}
	if len(got) != 2 || got[2001] != "node-a" || got[2002] != "node-b" {
		t.Fatalf("unexpected BatchLocateNode result: %+v", got)
	}
	if stub.nodeCalls != 1 || len(stub.lastNodeUIDs) != 2 || stub.lastNodeUIDs[0] != 2002 || stub.lastNodeUIDs[1] != 2003 {
		t.Fatalf("expected BatchLocateNode called once with missUIDs [2002 2003], got calls=%d uids=%v", stub.nodeCalls, stub.lastNodeUIDs)
	}

	allNodes, err := linker.BatchLocateNodes(context.Background(), []int64{2002, 2003})
	if err != nil || len(allNodes) != 1 || allNodes[2002]["monopoly01"] != "node-b" {
		t.Fatalf("unexpected BatchLocateNodes result: %+v err=%v", allNodes, err)
	}
}
