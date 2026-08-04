// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rls

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type snapshotTestCoord struct{}

type reconciliationTestCoord struct {
	metadataCalls atomic.Int32
	policies      []*rootcoordpb.RLSPolicyInfo
	principalTags map[string]map[string]string
	metadataErr   error
}

type blockingPolicyCoord struct {
	*reconciliationTestCoord
	started chan struct{}
	release chan struct{}
}

func (snapshotTestCoord) ShowCollectionIDs(context.Context, *rootcoordpb.ShowCollectionIDsRequest, ...grpc.CallOption) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	return &rootcoordpb.ShowCollectionIDsResponse{
		Status: merr.Success(),
		DbCollections: []*rootcoordpb.DBCollections{
			{DbName: "db", CollectionIDs: []int64{100}},
		},
	}, nil
}

func (snapshotTestCoord) GetRLSMetadata(context.Context, *rootcoordpb.GetRLSMetadataRequest, ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Policies: []*rootcoordpb.RLSPolicyInfo{
			{PolicyName: "tenant"},
		},
		Principals: []*rootcoordpb.RLSPrincipalInfo{
			{PrincipalName: "alice", Tags: map[string]string{"tenant": "acme"}},
		},
	}, nil
}

func (c *reconciliationTestCoord) ShowCollectionIDs(context.Context, *rootcoordpb.ShowCollectionIDsRequest, ...grpc.CallOption) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	return &rootcoordpb.ShowCollectionIDsResponse{
		Status: merr.Success(),
		DbCollections: []*rootcoordpb.DBCollections{
			{DbName: "db", CollectionIDs: []int64{100}},
		},
	}, nil
}

func (c *reconciliationTestCoord) GetRLSMetadata(context.Context, *rootcoordpb.GetRLSMetadataRequest, ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	c.metadataCalls.Add(1)
	if c.metadataErr != nil {
		return nil, c.metadataErr
	}
	principals := make([]*rootcoordpb.RLSPrincipalInfo, 0, len(c.principalTags))
	for principalName, tags := range c.principalTags {
		principals = append(principals, &rootcoordpb.RLSPrincipalInfo{
			PrincipalName: principalName,
			Tags:          tags,
		})
	}
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Policies:       c.policies,
		Principals:     principals,
	}, nil
}

func (c *blockingPolicyCoord) GetRLSMetadata(context.Context, *rootcoordpb.GetRLSMetadataRequest, ...grpc.CallOption) (*rootcoordpb.GetRLSMetadataResponse, error) {
	c.metadataCalls.Add(1)
	close(c.started)
	<-c.release
	return &rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Policies:       c.policies,
	}, nil
}

func TestManagerInitDoesNotBlockOnMetadataLoading(t *testing.T) {
	m := newManager()
	coord := &reconciliationTestCoord{}
	allocCalls := 0
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		allocCalls++
		return 10, nil
	}))
	require.Zero(t, allocCalls)
	require.Zero(t, coord.metadataCalls.Load())
	require.NotContains(t, m.collections, newCollectionKey(100))
}

func TestManagerEnsureFreshMetadataLoadsMissingSnapshots(t *testing.T) {
	m := newManager()
	coord := snapshotTestCoord{}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))
	require.NoError(t, m.ensureFreshMetadata(context.Background(), 100))

	state := m.collections[newCollectionKey(100)]
	require.NotNil(t, state)
	require.Contains(t, state.policies, "tenant")
	require.Equal(t, map[string]string{"tenant": "acme"}, state.principalTags["alice"])
}

func TestManagerEnsureFreshMetadataFailsClosed(t *testing.T) {
	m := newManager()
	coord := &reconciliationTestCoord{
		metadataErr: merr.WrapErrServiceUnavailableMsg("RLS metadata unavailable"),
	}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))

	err := m.ensureFreshMetadata(context.Background(), 100)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, int32(1), coord.metadataCalls.Load())
	require.NotContains(t, m.collections, newCollectionKey(100))
}

func TestManagerEnsureFreshMetadataSkipsFreshSnapshots(t *testing.T) {
	m := newManager()
	now := time.Now()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{Version: 10, RefreshedAt: now}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{Version: 10, RefreshedAt: now}))
	coord := &reconciliationTestCoord{
		metadataErr: merr.WrapErrServiceUnavailableMsg("refresh should not be called"),
	}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 20, nil
	}))

	require.NoError(t, m.ensureFreshMetadata(context.Background(), 100))
	require.Zero(t, coord.metadataCalls.Load())
}

func TestManagerEnsureFreshMetadataCoalescesConcurrentRefreshes(t *testing.T) {
	m := newManager()
	coord := &blockingPolicyCoord{
		reconciliationTestCoord: &reconciliationTestCoord{
			policies: []*rootcoordpb.RLSPolicyInfo{{PolicyName: "policy"}},
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	require.NoError(t, m.Init(context.Background(), coord, func(context.Context) (uint64, error) {
		return 10, nil
	}))

	const concurrency = 8
	errs := make(chan error, concurrency)
	for range concurrency {
		go func() {
			errs <- m.ensureFreshMetadata(context.Background(), 100)
		}()
	}
	select {
	case <-coord.started:
	case <-time.After(time.Second):
		t.Fatal("request-path RLS metadata refresh did not start")
	}
	require.Equal(t, int32(1), coord.metadataCalls.Load())
	close(coord.release)
	for range concurrency {
		require.NoError(t, <-errs)
	}
	require.Equal(t, int32(1), coord.metadataCalls.Load())
}

func TestManagerNotificationRefreshUpdatesOnlyRequestedSnapshot(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:  10,
		Policies: []*rlsutil.RowPolicy{{PolicyName: "old-policy"}},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       10,
		PrincipalTags: map[string]map[string]string{"alice": {"tenant": "old"}},
	}))

	coord := &reconciliationTestCoord{
		policies:      []*rootcoordpb.RLSPolicyInfo{{PolicyName: "new-policy"}},
		principalTags: map[string]map[string]string{"alice": {"tenant": "new"}},
	}
	require.NoError(t, m.RefreshPolicySnapshot(context.Background(), coord, "db", "coll", 100, 20))

	state := m.collections[newCollectionKey(100)]
	require.Equal(t, int64(20), state.policyVersion)
	require.Contains(t, state.policies, "new-policy")
	require.Equal(t, int64(10), state.principalTagVersion)
	require.Equal(t, map[string]string{"tenant": "old"}, state.principalTags["alice"])
}

func TestManagerSnapshotsUseSeparateVersionWatermarks(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 10,
		Policies: []*rlsutil.RowPolicy{
			{PolicyName: "new"},
		},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       5,
		PrincipalTags: map[string]map[string]string{"alice": {"team": "old"}},
	}))

	require.False(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 9,
		Policies: []*rlsutil.RowPolicy{
			{PolicyName: "stale"},
		},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       6,
		PrincipalTags: map[string]map[string]string{"alice": {"team": "new"}},
	}))

	state := m.collections[newCollectionKey(100)]
	require.Contains(t, state.policies, "new")
	require.NotContains(t, state.policies, "stale")
	require.Equal(t, map[string]string{"team": "new"}, state.principalTags["alice"])
}

func TestManagerRemoveCollection(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version: 1,
		Policies: []*rlsutil.RowPolicy{
			{PolicyName: "tenant"},
		},
	}))
	m.removeCollection(context.Background(), 100)
	require.NotContains(t, m.collections, newCollectionKey(100))
}

func TestManagerSnapshotsOwnImmutableData(t *testing.T) {
	m := newManager()
	policy := &rlsutil.RowPolicy{PolicyName: "tenant"}
	tags := map[string]string{"tenant": "acme"}
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:  1,
		Policies: []*rlsutil.RowPolicy{policy},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       1,
		PrincipalTags: map[string]map[string]string{"alice": tags},
	}))

	policy.PolicyName = "mutated"
	tags["tenant"] = "mutated"
	state := m.getCollectionState(newCollectionKey(100))
	require.NotNil(t, state)
	state.mu.RLock()
	defer state.mu.RUnlock()
	require.Contains(t, state.policies, "tenant")
	require.NotContains(t, state.policies, "mutated")
	require.Equal(t, "acme", state.principalTags["alice"]["tenant"])
}

func TestManagerCollectionStateLocksAreIndependent(t *testing.T) {
	m := newManager()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{Version: 1}))
	state := m.getCollectionState(newCollectionKey(100))
	require.NotNil(t, state)
	state.mu.Lock()
	defer state.mu.Unlock()

	done := make(chan bool, 1)
	go func() {
		done <- m.setRLSPolicySnapshot("db", 200, policySnapshot{Version: 1})
	}()
	select {
	case updated := <-done:
		require.True(t, updated)
	case <-time.After(time.Second):
		t.Fatal("updating one collection waited for another collection's state lock")
	}
}

func TestManagerReconcilesExpiredSnapshots(t *testing.T) {
	m := newManager()
	now := time.Now()
	oldRefresh := now.Add(-2 * time.Hour)
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:     10,
		RefreshedAt: oldRefresh,
		Policies:    []*rlsutil.RowPolicy{{PolicyName: "old-policy"}},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       10,
		RefreshedAt:   oldRefresh,
		PrincipalTags: map[string]map[string]string{"alice": {"tenant": "old"}},
	}))

	coord := &reconciliationTestCoord{
		policies:      []*rootcoordpb.RLSPolicyInfo{{PolicyName: "new-policy"}},
		principalTags: map[string]map[string]string{"alice": {"tenant": "new"}},
	}
	allocCalls := 0
	err := m.reconcileExpiredSnapshots(context.Background(), coord, func(context.Context) (uint64, error) {
		allocCalls++
		return 20, nil
	}, time.Hour, now)
	require.NoError(t, err)
	require.Equal(t, 1, allocCalls)
	require.Equal(t, int32(1), coord.metadataCalls.Load())

	state := m.collections[newCollectionKey(100)]
	require.Equal(t, int64(20), state.policyVersion)
	require.Equal(t, int64(20), state.principalTagVersion)
	require.Contains(t, state.policies, "new-policy")
	require.NotContains(t, state.policies, "old-policy")
	require.Equal(t, map[string]string{"tenant": "new"}, state.principalTags["alice"])
	require.True(t, state.policyLastSuccessfulRefresh.After(oldRefresh))
	require.True(t, state.principalTagLastSuccessfulRefresh.After(oldRefresh))
}

func TestManagerReconciliationSkipsFreshSnapshots(t *testing.T) {
	m := newManager()
	now := time.Now()
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{Version: 10, RefreshedAt: now}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{Version: 10, RefreshedAt: now}))

	coord := &reconciliationTestCoord{}
	allocCalls := 0
	err := m.reconcileExpiredSnapshots(context.Background(), coord, func(context.Context) (uint64, error) {
		allocCalls++
		return 20, nil
	}, time.Hour, now)
	require.NoError(t, err)
	require.Zero(t, allocCalls)
	require.Zero(t, coord.metadataCalls.Load())
}

func TestManagerReconciliationRetriesFailedBulkSnapshot(t *testing.T) {
	m := newManager()
	now := time.Now()
	oldRefresh := now.Add(-2 * time.Hour)
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:     10,
		RefreshedAt: oldRefresh,
		Policies:    []*rlsutil.RowPolicy{{PolicyName: "old-policy"}},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:       10,
		RefreshedAt:   oldRefresh,
		PrincipalTags: map[string]map[string]string{"alice": {"tenant": "old"}},
	}))

	coord := &reconciliationTestCoord{
		policies:      []*rootcoordpb.RLSPolicyInfo{{PolicyName: "new-policy"}},
		principalTags: map[string]map[string]string{"alice": {"tenant": "new"}},
		metadataErr:   merr.WrapErrServiceUnavailableMsg("RLS metadata unavailable"),
	}
	err := m.reconcileExpiredSnapshots(context.Background(), coord, func(context.Context) (uint64, error) {
		return 20, nil
	}, time.Hour, now)
	require.Error(t, err)

	state := m.collections[newCollectionKey(100)]
	require.Equal(t, int64(10), state.policyVersion)
	require.Contains(t, state.policies, "old-policy")
	require.Equal(t, int64(10), state.principalTagVersion)
	require.Equal(t, map[string]string{"tenant": "old"}, state.principalTags["alice"])

	coord.metadataErr = nil
	err = m.reconcileExpiredSnapshots(context.Background(), coord, func(context.Context) (uint64, error) {
		return 21, nil
	}, time.Hour, now)
	require.NoError(t, err)
	require.Equal(t, int64(21), state.policyVersion)
	require.Contains(t, state.policies, "new-policy")
	require.Equal(t, int64(21), state.principalTagVersion)
	require.Equal(t, map[string]string{"tenant": "new"}, state.principalTags["alice"])
	require.Equal(t, int32(2), coord.metadataCalls.Load())
}

func TestManagerReconciliationDoesNotOverwriteNewerNotification(t *testing.T) {
	m := newManager()
	now := time.Now()
	oldRefresh := now.Add(-2 * time.Hour)
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:     10,
		RefreshedAt: oldRefresh,
		Policies:    []*rlsutil.RowPolicy{{PolicyName: "old-policy"}},
	}))
	require.True(t, m.setRLSPrincipalTagsSnapshot("db", 100, principalTagsSnapshot{
		Version:     10,
		RefreshedAt: now,
	}))

	coord := &blockingPolicyCoord{
		reconciliationTestCoord: &reconciliationTestCoord{
			policies: []*rootcoordpb.RLSPolicyInfo{{PolicyName: "periodic-policy"}},
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() {
		done <- m.reconcileExpiredSnapshots(context.Background(), coord, func(context.Context) (uint64, error) {
			return 20, nil
		}, time.Hour, now)
	}()

	select {
	case <-coord.started:
	case <-time.After(time.Second):
		t.Fatal("periodic policy refresh did not start")
	}
	require.True(t, m.setRLSPolicySnapshot("db", 100, policySnapshot{
		Version:     30,
		RefreshedAt: now,
		Policies:    []*rlsutil.RowPolicy{{PolicyName: "notification-policy"}},
	}))
	close(coord.release)
	require.NoError(t, <-done)

	state := m.collections[newCollectionKey(100)]
	require.Equal(t, int64(30), state.policyVersion)
	require.Contains(t, state.policies, "notification-policy")
	require.NotContains(t, state.policies, "periodic-policy")
}

func TestManagerReconcilerStopsWithContext(t *testing.T) {
	m := newManager()
	coord := &reconciliationTestCoord{
		policies:      []*rootcoordpb.RLSPolicyInfo{{PolicyName: "policy"}},
		principalTags: map[string]map[string]string{"alice": {"tenant": "new"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- m.runReconciler(ctx, coord, func(context.Context) (uint64, error) {
			return 20, nil
		}, func() time.Duration {
			return 10 * time.Millisecond
		})
	}()

	require.Eventually(t, func() bool {
		return coord.metadataCalls.Load() > 0
	}, time.Second, 10*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("RLS snapshot reconciler did not stop after context cancellation")
	}
}
