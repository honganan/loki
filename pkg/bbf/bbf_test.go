package bbf

//func TestBBFManager(t *testing.T) {
//	m, err := CreateBBFManager("TestBBF", true)
//	assert.NoError(t, err)
//	defer os.RemoveAll("TestBBF")
//
//	prefix := "pre"
//	suffixes := []string{"chunk1"}
//
//	t.Run("test common usage", func(t *testing.T) {
//		resp1, err := m.Exists(context.Background(), &ExistsRequest{Filter: "test", Prefix: &prefix})
//		assert.NoError(t, err)
//		assert.False(t, resp1.Exists)
//
//		resp2, err := m.Create(context.Background(), &CreateRequest{Filter: "test", Prefix: &prefix, Capacity: 2 * 1024 * 1024})
//		assert.NoError(t, err)
//		assert.True(t, resp2.Created)
//
//		resp3, err := m.Add(context.Background(), &AddRequest{Filter: "test", Prefix: &prefix, Values: []string{"test1", "test2"}, Suffix: &suffixes[0]})
//		assert.NoError(t, err)
//		assert.True(t, resp3.Added)
//
//		resp4, err := m.BBFExists(context.Background(), &BBFExistsRequest{Filter: "test", Prefix: &prefix, Value: "test1", Suffixes: suffixes})
//		assert.NoError(t, err)
//		assert.True(t, resp4.Exists[0])
//	})
//
//	t.Run("test expansion", func(t *testing.T) {
//		resp2, err := m.Create(context.Background(), &CreateRequest{Filter: "test", Prefix: &prefix, Capacity: 2})
//		assert.NoError(t, err)
//		assert.True(t, resp2.Created)
//
//		resp3, err := m.Add(context.Background(), &AddRequest{Filter: "test", Prefix: &prefix, Values: []string{"test1", "test2", "test3"}, Suffix: &suffixes[0]})
//		assert.NoError(t, err)
//		assert.True(t, resp3.Added)
//
//		resp4, err := m.BBFExists(context.Background(), &BBFExistsRequest{Filter: "test", Prefix: &prefix, Value: "test1", Suffixes: suffixes})
//		assert.NoError(t, err)
//		assert.True(t, resp4.Exists[0])
//		resp4, err = m.BBFExists(context.Background(), &BBFExistsRequest{Filter: "test", Prefix: &prefix, Value: "test3", Suffixes: suffixes})
//		assert.NoError(t, err)
//		assert.True(t, resp4.Exists[0])
//	})
//
//	t.Run("test err rate", func(t *testing.T) {
//		resp, err := m.Create(context.Background(), &CreateRequest{Filter: "test", Prefix: &prefix, Capacity: 2000000})
//		assert.NoError(t, err)
//		assert.True(t, resp.Created)
//
//		tokens := make([]string, 2000000)
//		for i := range tokens {
//			tokens[i] = fmt.Sprintf("token_%d", i)
//		}
//		add, err := m.Add(context.Background(), &AddRequest{Filter: "test", Prefix: &prefix, Values: tokens, Suffix: &suffixes[0]})
//		assert.NoError(t, err)
//		assert.True(t, add.Added)
//
//		m.flush(true)
//
//		// 计算错误率
//		falsePositives := 0
//		noneExistsSuffix := make([]string, len(tokens))
//		for i := range tokens {
//			noneExistsSuffix[i] = fmt.Sprintf("non-existing-suffix_%d", i)
//		}
//		exists, err := m.BBFExists(context.Background(), &BBFExistsRequest{Filter: "test", Prefix: &prefix, Value: "token_0", Suffixes: noneExistsSuffix})
//		assert.NoError(t, err)
//		for _, e := range exists.Exists {
//			if e {
//				falsePositives++
//			}
//		}
//
//		p := float64(falsePositives) / float64(len(tokens))
//		maxFalsePositive := 0.0011
//		if p > maxFalsePositive {
//			t.Fatalf("too high false positive rate; got %.4f; want %.4f max", p, maxFalsePositive)
//		}
//	})
//}
//
//func TestBBF(t *testing.T) {
//	os.RemoveAll("TestBBF")
//	m, err := CreateBBFManager("TestBBF", true)
//	assert.NoError(t, err)
//	defer os.RemoveAll("TestBBF")
//	bbf, err := m.GetOrCreateBBF("test", "")
//	assert.NoError(t, err)
//
//	bbf.AddTo(0, []string{"test1", "test2"}, "")
//	assert.Equal(t, 4, len(bbf.Shards))
//	assert.Equal(t, 2, bbf.Meta.Appended)
//
//	contains, err := bbf.BBFExists("test1", []string{})
//	assert.NoError(t, err)
//	assert.True(t, contains[0])
//
//	contains, err = bbf.BBFExists("test2", []string{})
//	assert.NoError(t, err)
//	assert.True(t, contains[0])
//
//	contains, err = bbf.BBFExists("test3", []string{})
//	assert.NoError(t, err)
//	assert.False(t, contains[0])
//
//	dst := make([]byte, 0, 1024)
//	for _, shard := range bbf.Shards {
//		for _, filter := range shard.filters {
//			dst = filter.marshal(dst)
//		}
//	}
//	assert.Equal(t, 4*1024*1024+64, len(dst))
//
//	m.flush(true)
//	assert.FileExists(t, "./TestBBF/test/test_s0_f0")
//	assert.FileExists(t, "./TestBBF/test/test_s1_f0")
//	assert.FileExists(t, "./TestBBF/test/test_s2_f0")
//	assert.FileExists(t, "./TestBBF/test/test_s3_f0")
//
//	m.gc()
//}
//
//func TestBBFShard(t *testing.T) {
//	m, err := CreateBBFManager("TestBBFShard", false)
//	defer os.RemoveAll("TestBBFShard")
//	assert.NoError(t, err)
//
//	t.Run("test filter expand", func(t *testing.T) {
//		path := filepath.Join("TestBBFShard", m.getBBFDir("test"), "s1")
//		shard := newBBFShard(m, path, 4, nil)
//		shard.Add([]string{"test1", "test2", "test3", "test4"})
//		assert.Len(t, shard.filters, 1)
//		assert.Equal(t, 0, shard.maxIndex)
//		shard.Add([]string{"test5"})
//		assert.Len(t, shard.filters, 2)
//		shard.Add([]string{"test6"})
//		assert.Len(t, shard.filters, 2)
//		assert.Equal(t, 1, shard.maxIndex)
//
//		exists, err := shard.Contains([]string{"test1", "test2", "test3", "test4", "test5", "test6"})
//		assert.NoError(t, err)
//		for _, e := range exists {
//			assert.True(t, e)
//		}
//
//		tokens := make([]string, 2000)
//		for i := range tokens {
//			tokens[i] = fmt.Sprintf("token_%d", i)
//		}
//		shard.Add(tokens)
//		assert.Len(t, shard.filters, 3)
//		assert.Equal(t, 2, shard.maxIndex)
//	})
//
//	t.Run("test filter read before flush", func(t *testing.T) {
//		path := filepath.Join("TestBBFShard", m.getBBFDir("test"), "s1.1")
//		shard := newBBFShard(m, path, 4, nil)
//		shard.Add([]string{"test1", "test2", "test3", "test4"})
//		assert.Len(t, shard.filters, 1)
//		assert.Equal(t, 0, shard.maxIndex)
//		shard.Add([]string{"test5"})
//		assert.Len(t, shard.filters, 2)
//		shard.Add([]string{"test6"})
//		assert.Len(t, shard.filters, 2)
//		assert.Equal(t, 1, shard.maxIndex)
//
//		filters, err := shard.loadAllFilters()
//		assert.NoError(t, err)
//		assert.Len(t, filters, 2)
//	})
//
//	t.Run("test filter read with filter in memory and disk", func(t *testing.T) {
//		path := filepath.Join("TestBBFShard", m.getBBFDir("test"), "s1.2")
//		shard := newBBFShard(m, path, 4, nil)
//		shard.Add([]string{"test1", "test2", "test3", "test4"})
//		assert.Len(t, shard.filters, 1)
//		assert.Equal(t, 0, shard.maxIndex)
//
//		shard.mustFlush()
//		e, err := shard.Contains([]string{"test1"})
//		assert.NoError(t, err)
//		assert.True(t, e[0])
//
//		e, err = shard.Contains([]string{"test1"})
//		assert.NoError(t, err)
//		assert.True(t, e[0])
//
//		shard.Add([]string{"test5"})
//		assert.Len(t, shard.filters, 2)
//		shard.Add([]string{"test6"})
//		assert.Len(t, shard.filters, 2)
//		assert.Equal(t, 1, shard.maxIndex)
//
//		filters, err := shard.loadAllFilters()
//		assert.NoError(t, err)
//		assert.Len(t, filters, 2)
//	})
//
//	t.Run("test filter flush and update", func(t *testing.T) {
//		path := filepath.Join("TestBBFShard", m.getBBFDir("test"), "s1.3")
//		shard := newBBFShard(m, path, 4, nil)
//		shard.Add([]string{"test1", "test2"})
//		assert.Len(t, shard.filters, 1)
//		assert.Equal(t, 0, shard.maxIndex)
//
//		shard.mustFlush()
//		e, err := shard.Contains([]string{"test1"})
//		assert.NoError(t, err)
//		assert.True(t, e[0])
//
//		e, err = shard.Contains([]string{"test1"})
//		assert.NoError(t, err)
//		assert.True(t, e[0])
//
//		shard.Add([]string{"test3", "test4"})
//
//		filters, err := shard.loadAllFilters()
//		assert.NoError(t, err)
//		assert.Len(t, filters, 1)
//
//		assert.True(t, filters[0].containsAll([]string{"test1", "test2", "test3", "test4"}))
//		assert.False(t, filters[0].containsAll([]string{"test4", "test5"}))
//	})
//
//	t.Run("test filter flush", func(t *testing.T) {
//		path := filepath.Join(m.getBBFDir("test"), "s2")
//		shard := newBBFShard(m, path, 4, nil)
//		shard.Add([]string{"test1", "test2", "test3", "test4"})
//		assert.Len(t, shard.filters, 1)
//
//		shard.mustFlush()
//		assert.Len(t, shard.filters, 0)
//		assert.Equal(t, 0, shard.maxIndex)
//
//		e, err := shard.Contains([]string{"test1"})
//		assert.NoError(t, err)
//		assert.True(t, e[0])
//
//		e, err = shard.Contains([]string{"test1"})
//		assert.NoError(t, err)
//		assert.True(t, e[0])
//
//		filename := shard.getFilterFileName(shard.maxIndex)
//		assert.FileExists(t, filename)
//	})
//
//	t.Run("test filter reload", func(t *testing.T) {
//		path := filepath.Join(m.getBBFDir("test"), "s3")
//		shard := newBBFShard(m, path, 4, nil)
//		shard.Add([]string{"test1", "test2", "test3", "test4"})
//		shard.Add([]string{"test5", "test6", "test7", "test8"})
//		assert.Len(t, shard.filters, 2)
//
//		shard.mustFlush()
//		assert.Len(t, shard.filters, 0)
//		assert.Equal(t, 1, shard.maxIndex)
//
//		for i := 0; i <= shard.maxIndex; i++ {
//			filename := shard.getFilterFileName(i)
//			assert.FileExists(t, filename)
//		}
//
//		e, err := shard.Contains([]string{"test1"})
//		assert.NoError(t, err)
//		assert.True(t, e[0])
//
//		e, err = shard.Contains([]string{"test1"})
//		assert.NoError(t, err)
//		assert.True(t, e[0])
//
//		filters, err := shard.loadAllFilters()
//		assert.NoError(t, err)
//		assert.Len(t, filters, 2)
//
//		// read load
//		reloadShard := newBBFShard(m, path, 4, nil)
//		assert.Equal(t, -1, reloadShard.maxIndex)
//		filters, err = reloadShard.loadAllFilters()
//		assert.NoError(t, err)
//		assert.Equal(t, 1, reloadShard.maxIndex)
//		// filter not load to shard when read
//		assert.Len(t, shard.filters, 0)
//		assert.Len(t, filters, 2)
//		assert.True(t, filters[0].containsAll([]string{"test1", "test2", "test3", "test4"}))
//		assert.True(t, filters[1].containsAll([]string{"test5", "test6", "test7", "test8"}))
//
//		//write load
//		reloadShard = newBBFShard(m, path, 4, nil)
//		assert.Equal(t, -1, reloadShard.maxIndex)
//		filter, err := reloadShard.loadNewestFilter()
//		assert.NoError(t, err)
//		assert.Equal(t, 1, reloadShard.maxIndex)
//		assert.Len(t, filters, 2)
//		assert.True(t, filter.containsAll([]string{"test5", "test6", "test7", "test8"}))
//	})
//
//	t.Run("flush then update", func(t *testing.T) {
//		path := filepath.Join("TestBBFShard", m.getBBFDir("test"), "s4")
//		shard := newBBFShard(m, path, 4, nil)
//		shard.Add([]string{"test1", "test2", "test3", "test4"})
//		shard.Add([]string{"test5", "test6"})
//		assert.Len(t, shard.filters, 2)
//
//		shard.mustFlush()
//		assert.Len(t, shard.filters, 0)
//		assert.Equal(t, 1, shard.maxIndex)
//
//		for i := 0; i <= shard.maxIndex; i++ {
//			filename := shard.getFilterFileName(i)
//			assert.FileExists(t, filename)
//		}
//
//		// add new data
//		shard.Add([]string{"test7", "test8"})
//		assert.Equal(t, 1, shard.maxIndex)
//
//		for i := 0; i <= shard.maxIndex; i++ {
//			filename := shard.getFilterFileName(i)
//			assert.FileExists(t, filename)
//		}
//		shard.mustFlush()
//		e, err := shard.Contains([]string{"test7"})
//		assert.NoError(t, err)
//		assert.True(t, e[0])
//
//		e, err = shard.Contains([]string{"test7"})
//		assert.NoError(t, err)
//		assert.True(t, e[0])
//	})
//}
//
//func CreateBBFManager(path string, enableCache bool) (*BBFManager, error) {
//	cacheConfig := cache.Config{
//		EmbeddedCache: cache.EmbeddedCacheConfig{
//			Enabled:       enableCache,
//			MaxSizeMB:     100,
//			TTL:           1 * time.Nanosecond,
//			PurgeInterval: 120 * time.Second,
//		},
//	}
//	cfg := Config{
//		Path:            path,
//		ShardCapacity:   1_000_000,
//		FlushInterval:   20 * time.Minute,
//		GcInterval:      10 * time.Hour,
//		Retention:       1000 * time.Hour,
//		CacheCfg:        cacheConfig,
//	}
//	return NewBBFManger(cfg, nil, nil, prometheus.NewRegistry(), nil, util_log.Logger, stats.IndexCache)
//}
