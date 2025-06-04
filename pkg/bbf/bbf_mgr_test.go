package bbf

//
//import (
//	"context"
//	"os"
//	"testing"
//
//	"github.com/pkg/errors"
//	"github.com/stretchr/testify/assert"
//)
//
//func newTestBBFManager(path string) *BBFManager {
//	ctx, cancel := context.WithCancel(context.Background())
//	return &BBFManager{
//		ctx:    ctx,
//		cancel: cancel,
//		bbfs:   make(map[string]*BBF),
//		path:   path,
//		shards: 1,
//	}
//}
//
//func TestOSStat(t *testing.T) {
//	_, err := os.Stat("/tmp/NOTEXISTS.file")
//	notExtErr := os.IsNotExist(err)
//	extErr := os.IsExist(err)
//	assert.True(t, notExtErr)
//	assert.False(t, extErr)
//}
//
//func TestGetOrAddBBF(t *testing.T) {
//	path := "TestGetOrAddBBF"
//	defer os.RemoveAll(path)
//	os.RemoveAll(path)
//	m := newTestBBFManager(path)
//
//	err := m.init()
//	assert.Nil(t, err)
//
//	t.Run("get and del bbf", func(t *testing.T) {
//		bbf, err := m.GetOrCreateBBF("test", "")
//		assert.Nil(t, err)
//
//		metaFile := m.getMetaFilename("test")
//		assert.FileExists(t, metaFile)
//
//		_, ok := m.bbfs[metaFile]
//		assert.True(t, ok)
//
//		loadBBF, err := m.bbfFromFile(metaFile)
//		assert.Nil(t, err)
//
//		assert.Equal(t, bbf.Meta, loadBBF.Meta)
//
//		err = m.deleteBBF(bbf)
//		assert.Nil(t, err)
//
//		assert.NoFileExists(t, metaFile)
//
//		_, ok = m.bbfs[metaFile]
//		assert.False(t, ok)
//
//		bbf, err = m.GetBBF("test")
//		assert.Nil(t, bbf)
//		assert.True(t, errors.Is(err, bbfNotFoundErr))
//	})
//
//	t.Run("init", func(t *testing.T) {
//		bbf1, err := m.GetOrCreateBBF("test1", "")
//		assert.Nil(t, err)
//
//		metaFile1 := m.getMetaFilename("test1")
//		assert.FileExists(t, metaFile1)
//
//		bbf2, err := m.GetOrCreateBBF("test2", "")
//		assert.Nil(t, err)
//
//		metaFile2 := m.getMetaFilename("test2")
//		assert.FileExists(t, metaFile2)
//
//		newM := newTestBBFManager(path)
//		err = newM.init()
//		assert.Nil(t, err)
//
//		rbbf1, ok := newM.bbfs[metaFile1]
//		assert.True(t, ok)
//		assert.Equal(t, bbf1.Meta, rbbf1.Meta)
//
//		rbbf2, ok := newM.bbfs[metaFile2]
//		assert.True(t, ok)
//		assert.Equal(t, bbf2.Meta, rbbf2.Meta)
//	})
//
//	t.Run("load invalid bbf", func(t *testing.T) {
//		metaFile3 := m.getMetaFilename("test3")
//		assert.NoFileExists(t, metaFile3)
//		err := os.WriteFile(metaFile3, []byte("invalid"), 0644)
//		assert.Nil(t, err)
//
//		_, err = m.GetOrCreateBBF("test4", "")
//		assert.Nil(t, err)
//		metaFile4 := m.getMetaFilename("test4")
//
//		newM := newTestBBFManager(path)
//		err = newM.init()
//		assert.Nil(t, err)
//
//		_, ok := newM.bbfs[metaFile3]
//		assert.False(t, ok)
//
//		_, ok = newM.bbfs[metaFile4]
//		assert.True(t, ok)
//	})
//
//}
