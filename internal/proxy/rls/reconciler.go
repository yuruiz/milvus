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
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type reconciliationTarget struct {
	dbName               string
	collectionID         UniqueID
	refreshPolicies      bool
	refreshPrincipalTags bool
}

func (m *manager) RunReconciler(ctx context.Context, coord CoordClient, allocVersion SnapshotVersionAllocator) error {
	return m.runReconciler(ctx, coord, allocVersion, func() time.Duration {
		return paramtable.Get().ProxyCfg.RLSMetaRefreshInterval.GetAsDuration(time.Second)
	})
}

func (m *manager) runReconciler(ctx context.Context, coord CoordClient, allocVersion SnapshotVersionAllocator, refreshInterval func() time.Duration) error {
	if m == nil || coord == nil || allocVersion == nil || refreshInterval == nil {
		return merr.WrapErrServiceInternalMsg("failed to start RLS snapshot reconciler without required dependencies")
	}

	interval := refreshInterval()
	if interval <= 0 {
		return merr.WrapErrServiceInternalMsg("failed to start RLS snapshot reconciler with invalid interval %s", interval)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	if err := m.reconcileExpiredSnapshots(ctx, coord, allocVersion, interval, time.Now()); err != nil {
		mlog.RatedWarn(ctx, rate.Limit(1), "failed to initialize RLS snapshots", mlog.Err(err))
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if err := m.reconcileExpiredSnapshots(ctx, coord, allocVersion, interval, now); err != nil {
				mlog.RatedWarn(ctx, rate.Limit(1), "failed to reconcile expired RLS snapshots", mlog.Err(err))
			}

			newInterval := refreshInterval()
			if newInterval <= 0 {
				mlog.RatedWarn(ctx, rate.Limit(1), "ignore invalid RLS snapshot refresh interval", mlog.Duration("interval", newInterval))
				continue
			}
			if newInterval != interval {
				interval = newInterval
				ticker.Reset(interval)
			}
		}
	}
}

func (m *manager) reconcileExpiredSnapshots(ctx context.Context, coord CoordClient, allocVersion SnapshotVersionAllocator, refreshTTL time.Duration, now time.Time) error {
	if m == nil || coord == nil || allocVersion == nil {
		return merr.WrapErrServiceInternalMsg("failed to reconcile RLS snapshots without required dependencies")
	}
	if refreshTTL <= 0 {
		return merr.WrapErrServiceInternalMsg("failed to reconcile RLS snapshots with invalid TTL %s", refreshTTL)
	}

	targets, err := m.listExpiredSnapshotTargets(ctx, coord, refreshTTL, now)
	if err != nil || len(targets) == 0 {
		return err
	}

	version, err := allocVersion(ctx)
	if err != nil {
		return merr.Wrap(err, "failed to allocate RLS snapshot reconciliation version")
	}

	var reconcileErr error
	for _, target := range targets {
		var err error
		switch {
		case target.refreshPolicies && target.refreshPrincipalTags:
			err = m.refreshCollectionSnapshot(ctx, coord, target.dbName, "", target.collectionID, version)
		case target.refreshPolicies:
			err = m.RefreshPolicySnapshot(ctx, coord, target.dbName, "", target.collectionID, version)
		case target.refreshPrincipalTags:
			err = m.RefreshPrincipalTagsSnapshot(ctx, coord, target.dbName, "", target.collectionID, version)
		}
		if err != nil {
			reconcileErr = errors.CombineErrors(reconcileErr, merr.Wrapf(err, "failed to reconcile RLS snapshots for %s/%d", target.dbName, target.collectionID))
		}
	}
	return reconcileErr
}

func (m *manager) listExpiredSnapshotTargets(ctx context.Context, coord CoordClient, refreshTTL time.Duration, now time.Time) ([]reconciliationTarget, error) {
	resp, err := coord.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		AllowUnavailable: false,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, merr.Wrap(err, "failed to list collections for RLS snapshot reconciliation")
	}

	targets := make([]reconciliationTarget, 0)
	for _, dbCollections := range resp.GetDbCollections() {
		for _, collectionID := range dbCollections.GetCollectionIDs() {
			refreshPolicies, refreshPrincipalTags := m.snapshotRefreshDue(collectionID, refreshTTL, now)
			if !refreshPolicies && !refreshPrincipalTags {
				continue
			}
			targets = append(targets, reconciliationTarget{
				dbName:               dbCollections.GetDbName(),
				collectionID:         collectionID,
				refreshPolicies:      refreshPolicies,
				refreshPrincipalTags: refreshPrincipalTags,
			})
		}
	}
	return targets, nil
}

func (m *manager) snapshotRefreshDue(collectionID UniqueID, refreshTTL time.Duration, now time.Time) (bool, bool) {
	state := m.getCollectionState(newCollectionKey(collectionID))
	if state == nil {
		return true, true
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	policyDue := state.policyLastSuccessfulRefresh.IsZero() || !state.policyLastSuccessfulRefresh.Add(refreshTTL).After(now)
	principalDue := state.principalTagLastSuccessfulRefresh.IsZero() || !state.principalTagLastSuccessfulRefresh.Add(refreshTTL).After(now)
	return policyDue, principalDue
}
