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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestProxyRLSInvalidateRefreshesPolicySnapshot(t *testing.T) {
	ctx := context.Background()
	mixCoord := &mocks.MockMixCoordClient{}
	t.Cleanup(func() {
		mixCoord.AssertExpectations(t)
	})
	node := &Proxy{mixCoord: mixCoord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	mixCoord.EXPECT().GetRLSMetadata(mock.Anything, mock.MatchedBy(func(req *rootcoordpb.GetRLSMetadataRequest) bool {
		return req != nil && req.GetCollectionId() == 100
	})).Return(&rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Policies:       []*rootcoordpb.RLSPolicyInfo{{PolicyName: "tenant"}},
	}, nil).Once()

	status, err := node.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
		Base:           &commonpb.MsgBase{MsgType: rlsutil.MsgTypeCreateRowPolicy, Timestamp: 10},
		DbName:         "db",
		CollectionName: "coll",
		CollectionID:   100,
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
}

func TestProxyRLSInvalidateRefreshesPrincipalTagsSnapshot(t *testing.T) {
	ctx := context.Background()
	mixCoord := &mocks.MockMixCoordClient{}
	t.Cleanup(func() {
		mixCoord.AssertExpectations(t)
	})
	node := &Proxy{mixCoord: mixCoord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	mixCoord.EXPECT().GetRLSMetadata(mock.Anything, mock.MatchedBy(func(req *rootcoordpb.GetRLSMetadataRequest) bool {
		return req != nil && req.GetCollectionId() == 100
	})).Return(&rootcoordpb.GetRLSMetadataResponse{
		Status:         merr.Success(),
		DbName:         "db",
		CollectionName: "coll",
		CollectionId:   100,
		Principals: []*rootcoordpb.RLSPrincipalInfo{
			{PrincipalName: "alice", Tags: map[string]string{"tenant": "acme"}},
		},
	}, nil).Once()

	status, err := node.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
		Base:           &commonpb.MsgBase{MsgType: rlsutil.MsgTypeSetRLSPrincipalTags, Timestamp: 10},
		DbName:         "db",
		CollectionName: "coll",
		CollectionID:   100,
	})
	require.NoError(t, err)
	require.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
}
