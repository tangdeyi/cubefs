package master

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_AllocFileID(t *testing.T) {
	idAlloc := server.cluster.idAlloc

	start := idAlloc.fileId
	id, err := idAlloc.allocateFileId(100)
	require.NoError(t, err)

	require.Equal(t, id.Begin, start)
	require.Equal(t, id.End, start+100)
	require.Equal(t, id.End, idAlloc.fileId)

	idAlloc.restoreFileID()
	require.Equal(t, id.End, idAlloc.fileId)
}
