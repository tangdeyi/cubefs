// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package priority

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetpriority(t *testing.T) {
	pri := GetPriority(0)
	require.Equal(t, normal, pri)
	require.Equal(t, "normal", pri.String())

	pri = GetPriority(10)
	require.Equal(t, background, pri)
	require.Equal(t, "background", pri.String())

	pri = GetPriority(1)
	require.Equal(t, background, pri)
	require.Equal(t, "background", pri.String())
}
