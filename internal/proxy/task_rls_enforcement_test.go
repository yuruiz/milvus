// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/rls"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func newRLSOperationTestSchema(collectionName string) *schemaInfo {
	return mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "value",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  102,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "2"},
				},
			},
		},
	})
}

func installRLSOperationTestCache(t *testing.T, collectionID int64, schema *schemaInfo) {
	originalCache := globalMetaCache
	t.Cleanup(func() {
		globalMetaCache = originalCache
	})

	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(collectionID, nil).Maybe()
	cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
		collID:     collectionID,
		schema:     schema,
		rlsEnabled: true,
	}, nil).Maybe()
	cache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schema, nil).Maybe()
	cache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 1}, nil).Maybe()
	cache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&partitionInfo{
		name:        paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		partitionID: 10,
	}, nil).Maybe()
	cache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{
		paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(): 10,
	}, nil).Maybe()
	globalMetaCache = cache
}

func refreshRLSOperationTestMetadata(t *testing.T, collectionID int64, policies []*rlsutil.RowPolicy) {
	ctx := context.Background()
	rls.RemoveCollection(ctx, collectionID)
	t.Cleanup(func() {
		rls.RemoveCollection(ctx, collectionID)
	})

	coord := mocks.NewMockMixCoordClient(t)
	coord.EXPECT().GetRLSMetadata(mock.Anything, mock.Anything).Return(&rootcoordpb.GetRLSMetadataResponse{
		Status:       merr.Success(),
		CollectionId: collectionID,
		Policies:     rlsPolicyInfos(policies),
	}, nil).Twice()
	require.NoError(t, rls.DefaultManager().RefreshPolicySnapshot(ctx, coord, "default", "rls_collection", collectionID, 1))
	require.NoError(t, rls.DefaultManager().RefreshPrincipalTagsSnapshot(ctx, coord, "default", "rls_collection", collectionID, 1))
}

func rlsPolicyInfos(policies []*rlsutil.RowPolicy) []*rootcoordpb.RLSPolicyInfo {
	converted := make([]*rootcoordpb.RLSPolicyInfo, 0, len(policies))
	for _, policy := range policies {
		if policy == nil {
			continue
		}
		actions := make([]int32, len(policy.Actions))
		for i, action := range policy.Actions {
			actions[i] = int32(action)
		}
		converted = append(converted, &rootcoordpb.RLSPolicyInfo{
			PolicyName:  policy.PolicyName,
			PolicyType:  int32(policy.PolicyType),
			Actions:     actions,
			UsingExpr:   policy.UsingExpr,
			CheckExpr:   policy.CheckExpr,
			Description: policy.Description,
			PolicyId:    policy.PolicyId,
		})
	}
	return converted
}

func newRLSOperationTestAllocator(t *testing.T) *allocator.IDAllocator {
	ctx := context.Background()
	coord := mocks.NewMockRootCoordClient(t)
	coord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     1000,
		Count:  100,
	}, nil).Maybe()
	idAllocator, err := allocator.NewIDAllocator(ctx, coord, paramtable.GetNodeID())
	require.NoError(t, err)
	idAllocator.Start()
	t.Cleanup(idAllocator.Close)
	return idAllocator
}

func newRLSOperationTestFieldsData() []*schemapb.FieldData {
	return []*schemapb.FieldData{
		{
			FieldName: "id",
			FieldId:   100,
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1}},
					},
				},
			},
		},
		{
			FieldName: "value",
			FieldId:   101,
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{10}},
					},
				},
			},
		},
		{
			FieldName: "vector",
			FieldId:   102,
			Type:      schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 2,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{Data: []float32{0.1, 0.2}},
					},
				},
			},
		},
	}
}

func TestQueryTaskRLSEnforcement(t *testing.T) {
	const collectionID = int64(991001)
	const collectionName = "rls_query_collection"
	ctx := context.Background()
	schema := newRLSOperationTestSchema(collectionName)
	installRLSOperationTestCache(t, collectionID, schema)

	newTask := func(t *testing.T, principalName string) *queryTask {
		task := &queryTask{
			Condition: NewTaskCondition(ctx),
			RetrieveRequest: &internalpb.RetrieveRequest{
				Base: &commonpb.MsgBase{},
			},
			ctx: ctx,
			request: &milvuspb.QueryRequest{
				CollectionName: collectionName,
				Expr:           "id > 0",
				RlsPrincipal:   principalName,
			},
		}
		require.NoError(t, task.OnEnqueue())
		return task
	}

	t.Run("missing principal", func(t *testing.T) {
		err := newTask(t, "").PreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})

	t.Run("merge using predicate", func(t *testing.T) {
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{
			{
				PolicyName: "query_policy",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.RowPolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "id < 10",
			},
		})
		task := newTask(t, "alice")
		require.NoError(t, task.PreExecute(ctx))
		predicate := task.plan.GetQuery().GetPredicates()
		require.NotNil(t, predicate)
		expected, err := planparserv2.ParseExpr(schema.schemaHelper, "id > 0 and id < 10", nil)
		require.NoError(t, err)
		require.True(t, proto.Equal(expected, predicate), "unexpected merged predicate: %s", predicate.String())
	})
}

func TestSearchTaskRLSEnforcementRequiresPrincipal(t *testing.T) {
	const collectionID = int64(991002)
	const collectionName = "rls_search_collection"
	ctx := context.Background()
	installRLSOperationTestCache(t, collectionID, newRLSOperationTestSchema(collectionName))
	searchParams := getValidSearchParams()
	resetSearchParamsValue(searchParams, AnnsFieldKey, "vector")

	task := &searchTask{
		ctx:            ctx,
		collectionName: collectionName,
		SearchRequest:  &internalpb.SearchRequest{},
		request: &milvuspb.SearchRequest{
			CollectionName: collectionName,
			Nq:             1,
			Dsl:            "id > 0",
			DslType:        commonpb.DslType_BoolExprV1,
			SearchParams:   searchParams,
		},
		tr: timerecord.NewTimeRecorder("rls-search-test"),
	}
	require.NoError(t, task.OnEnqueue())

	err := task.PreExecute(ctx)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}

func TestDeleteRunnerRLSEnforcementRequiresPrincipal(t *testing.T) {
	const collectionID = int64(991003)
	const collectionName = "rls_delete_collection"
	ctx := context.Background()
	installRLSOperationTestCache(t, collectionID, newRLSOperationTestSchema(collectionName))

	runner := &deleteRunner{
		req: &milvuspb.DeleteRequest{
			DbName:         "default",
			CollectionName: collectionName,
			Expr:           "id == 1",
		},
	}

	err := runner.Init(ctx)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}

func TestInsertTaskRLSEnforcement(t *testing.T) {
	const collectionID = int64(991004)
	const collectionName = "rls_insert_collection"
	ctx := context.Background()
	schema := newRLSOperationTestSchema(collectionName)
	installRLSOperationTestCache(t, collectionID, schema)

	newTask := func(t *testing.T, principalName string) *insertTask {
		task := &insertTask{
			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			insertMsg: &BaseInsertTask{
				InsertRequest: &msgpb.InsertRequest{
					Base:           &commonpb.MsgBase{},
					DbName:         "default",
					CollectionName: collectionName,
					PartitionName:  paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
					FieldsData:     newRLSOperationTestFieldsData(),
					NumRows:        1,
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},
			idAllocator:  newRLSOperationTestAllocator(t),
			rlsPrincipal: principalName,
		}
		require.NoError(t, task.OnEnqueue())
		return task
	}

	t.Run("missing principal", func(t *testing.T) {
		err := newTask(t, "").PreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})

	t.Run("reject row violating check predicate", func(t *testing.T) {
		refreshRLSOperationTestMetadata(t, collectionID, []*rlsutil.RowPolicy{
			{
				PolicyName: "insert_policy",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.RowPolicyAction{rlsutil.PolicyActionInsert},
				CheckExpr:  "id < 0",
			},
		})
		err := newTask(t, "alice").PreExecute(ctx)
		require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	})
}

func TestUpsertTaskRLSEnforcementRequiresPrincipal(t *testing.T) {
	const collectionID = int64(991005)
	ctx := context.Background()
	task := createTestUpdateTask()
	installRLSOperationTestCache(t, collectionID, task.schema)
	require.NoError(t, task.OnEnqueue())

	err := task.PreExecute(ctx)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}
