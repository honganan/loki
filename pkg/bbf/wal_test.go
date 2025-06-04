package bbf

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWAL(t *testing.T) {
	t.Run("WriteAndRead", func(t *testing.T) {
		filePath := "/tmp/test_WriteAndRead_wal.log"
		maxSize := int64(1024)
		wal, err := NewWAL(context.Background(), filePath, maxSize)
		require.NoError(t, err)
		defer func() {
			files, _ := filepath.Glob(filePath + ".*")
			for _, file := range files {
				os.Remove(file)
			}
		}()

		chunkIDs := []string{"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000001",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000002",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000003"}
		err = wal.Write(chunkIDs)
		require.NoError(t, err)

		chunkChan := make(chan string, len(chunkIDs))
		go func() {
			err := wal.Read(1000, 1, func(ctx context.Context, chunks []string) {
				for _, chunk := range chunks {
					chunkChan <- chunk
				}
			})
			require.NoError(t, err)
		}()

		for _, expectedChunkID := range chunkIDs {
			select {
			case chunkID := <-chunkChan:
				require.Equal(t, expectedChunkID, chunkID)
			case <-time.After(1 * time.Second):
				t.Errorf("Timeout waiting for chunkID %s", expectedChunkID)
			}
		}
	})

	t.Run("Rotate", func(t *testing.T) {
		filePath := "/tmp/test_Rotate_wal.log"
		maxSize := int64(100)
		wal, err := NewWAL(context.Background(), filePath, maxSize)
		require.NoError(t, err)
		defer func() {
			files, _ := filepath.Glob(filePath + ".*")
			for _, file := range files {
				os.Remove(file)
			}
		}()

		chunkIDs := []string{"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000001",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000002",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000003"}
		for _, chunkID := range chunkIDs {
			err = wal.Write([]string{chunkID})
			require.NoError(t, err)
		}

		fs, err := filepath.Glob(fmt.Sprintf("%s.*", filePath))
		require.NoError(t, err)
		require.Equal(t, 4, len(fs))
	})

	t.Run("Offset", func(t *testing.T) {
		filePath := "/tmp/test_Offset_wal.log"
		maxSize := int64(1024)
		wal, err := NewWAL(context.Background(), filePath, maxSize)
		require.NoError(t, err)
		defer func() {
			err := wal.Close()
			require.NoError(t, err)
			files, _ := filepath.Glob(filePath + ".*")
			for _, file := range files {
				os.Remove(file)
			}
		}()

		initialChunkIDs := []string{"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000001",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000002",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000003"}
		err = wal.Write(initialChunkIDs)
		require.NoError(t, err)

		err = saveOffset(wal.walFileName+".offset", 55*3)
		require.NoError(t, err)

		additionalChunkIDs := []string{"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000004",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000005"}
		err = wal.Write(additionalChunkIDs)
		require.NoError(t, err)
		wal.Close()

		wal, err = NewWAL(context.Background(), filePath, maxSize)
		require.NoError(t, err)

		chunkChan := make(chan string, len(initialChunkIDs))
		go wal.Read(1000, 1, func(ctx context.Context, chunks []string) {
			for _, chunk := range chunks {
				chunkChan <- chunk
			}
		})

		for _, expectedChunkID := range additionalChunkIDs {
			select {
			case chunkID := <-chunkChan:
				require.Equal(t, expectedChunkID, chunkID)
			case <-time.After(1 * time.Second):
				t.Errorf("Timeout waiting for chunkID %s", expectedChunkID)
			}
		}
	})

	t.Run("RotateAndReadOldFile", func(t *testing.T) {
		filePath := "/tmp/test_RotateAndReadOldFile_wal.log"
		maxSize := int64(55 * 1024 * 1024)
		wal, err := NewWAL(context.Background(), filePath, maxSize)
		require.NoError(t, err)
		defer func() {
			files, _ := filepath.Glob(filePath + ".*")
			for _, file := range files {
				os.Remove(file)
			}
		}()

		chunkChan := make(chan string, 1000)
		go func() {
			err := wal.Read(1000, 20, func(ctx context.Context, chunks []string) {
				for _, chunk := range chunks {
					chunkChan <- chunk
				}
			})
			require.NoError(t, err)
		}()

		initialChunkIDs := make([]string, 0)
		for i := 0; i < 150_000; i++ {
			initialChunkIDs = append(initialChunkIDs, fmt.Sprintf("fake/48294f1d31cc19a7/192284ff543:192285040d8:000%d", i))
		}
		for _, chunkID := range initialChunkIDs {
			err = wal.Write([]string{chunkID})
			require.NoError(t, err)
		}

		_, err = filepath.Glob(fmt.Sprintf("%s.%s", filePath, "202*"))
		require.NoError(t, err)

		additionalChunkIDs := make([]string, 0)
		for i := 150_000; i < 160_000; i++ {
			additionalChunkIDs = append(additionalChunkIDs, fmt.Sprintf("fake/48294f1d31cc19a7/192284ff543:192285040d8:0000000%d", i))
		}
		err = wal.Write(additionalChunkIDs)
		require.NoError(t, err)

		expectedChunkIDs := append(initialChunkIDs, additionalChunkIDs...)

		for _, expectedChunkID := range expectedChunkIDs {
			select {
			case chunkID := <-chunkChan:
				require.Equal(t, expectedChunkID, chunkID)
			case <-time.After(2 * time.Second):
				t.Errorf("Timeout waiting for chunkID %s", expectedChunkID)
			}
		}
	})

	t.Run("Read_SplitsChunksByBucket", func(t *testing.T) {
		filePath := "/tmp/test_SplitsChunksByBucket_wal.log"
		maxSize := int64(55 * 1024 * 1024)
		wal, err := NewWAL(context.Background(), filePath, maxSize)
		require.NoError(t, err)
		defer func() {
			files, _ := filepath.Glob(filePath + ".*")
			for _, file := range files {
				os.Remove(file)
			}
		}()

		chunkIDs := []string{
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000001",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000002",
			"fake/48294f1d31cc19a7/192284ff543:192286040d8:00000003",
			"fake/48294f1d31cc19a7/192284ff543:192286040d8:00000004",
		}
		err = wal.Write(chunkIDs)
		require.NoError(t, err)

		processedChunks := make([][]string, 0, 2)
		compute := func(ctx context.Context, chunks []string) {
			processedChunks = append(processedChunks, chunks)
		}

		go func() {
			compute(context.Background(), chunkIDs[:2])
			compute(context.Background(), chunkIDs[2:])
		}()

		time.Sleep(3 * time.Second)

		require.Len(t, processedChunks, 2)
		require.ElementsMatch(t, processedChunks[0], chunkIDs[:2])
		require.ElementsMatch(t, processedChunks[1], chunkIDs[2:])
	})

	t.Run("Read_SortsAndGroupsChunksByBucket", func(t *testing.T) {
		filePath := "/tmp/test_SortsAndGroupsChunksByBucket_wal.log"
		maxSize := int64(55 * 1024 * 1024)
		wal, err := NewWAL(context.Background(), filePath, maxSize)
		require.NoError(t, err)
		defer func() {
			files, _ := filepath.Glob(filePath + ".*")
			for _, file := range files {
				os.Remove(file)
			}
		}()

		chunkIDs := []string{
			"fake/48294f1d31cc19a7/192286040d8:192287040d8:00000003",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000001",
			"fake/48294f1d31cc19a7/192286040d8:192287040d8:00000004",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000002",
		}
		err = wal.Write(chunkIDs)
		require.NoError(t, err)

		processedChunks := make([][]string, 0)
		compute := func(ctx context.Context, chunks []string) {
			processedChunks = append(processedChunks, chunks)
		}

		go func() {
			err := wal.Read(1000, 20, compute)
			require.NoError(t, err)
		}()

		time.Sleep(2 * time.Second)

		require.Len(t, processedChunks, 2)
		require.ElementsMatch(t, processedChunks[0], []string{
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000001",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000002",
		})
		require.ElementsMatch(t, processedChunks[1], []string{
			"fake/48294f1d31cc19a7/192286040d8:192287040d8:00000003",
			"fake/48294f1d31cc19a7/192286040d8:192287040d8:00000004",
		})
	})

	t.Run("GroupChunkIDs", func(t *testing.T) {
		chunkIDs := []string{
			"fake/48294f1d31cc19a7/192286040d8:192287040d8:00000003",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000001",
			"fake/48294f1d31cc19a7/192286040d8:192287040d8:00000004",
			"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000002",
		}

		expected := [][]string{
			{
				"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000001",
				"fake/48294f1d31cc19a7/192284ff543:192285040d8:00000002",
			},
			{
				"fake/48294f1d31cc19a7/192286040d8:192287040d8:00000003",
				"fake/48294f1d31cc19a7/192286040d8:192287040d8:00000004",
			},
		}

		result := groupChunkIDs(chunkIDs)
		require.Equal(t, expected, result)
	})
}
