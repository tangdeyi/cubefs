package metanode

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDentry_Marshal(t *testing.T) {
	dentry := &Dentry{
		ParentId: 11,
		Name:     "test",
		Inode:    101,
		Type:     0x644,
		FileId:   1110,
	}

	data, err := dentry.Marshal()
	require.NoError(t, err)

	newDentry := &Dentry{}
	err = newDentry.Unmarshal(data)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(dentry, newDentry))

	// test unmarshal from old data
	valData := dentry.MarshalValue()
	tmpData := valData[:12]
	err = newDentry.UnmarshalValue(tmpData)
	require.NoError(t, err)
	require.Equal(t, dentry.Inode, newDentry.Inode)

	// old data from snapshot
	tmpData = valData[:44]
	err = newDentry.UnmarshalValue(tmpData)
	require.NoError(t, err)
	require.Equal(t, dentry.Inode, newDentry.Inode)
}
