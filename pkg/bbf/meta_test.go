package bbf

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	slices.Sort(a)
	slices.Sort(b)

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mapEqual(m1, m2 map[string][]string) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k, v := range m1 {
		if !sliceEqual(v, m2[k]) {
			return false
		}
	}
	return true
}

func TestMetas_addObject(t *testing.T) {
	type args struct {
		contents []string
	}
	tests := []struct {
		name string
		args args
		want map[string][]string
	}{
		{
			name: "test0",
			args: args{
				contents: []string{},
			},
			want: map[string][]string{},
		},
		{
			name: "test1",
			args: args{
				contents: []string{"BBF/202410120140/s994_f0"},
			},
			want: map[string][]string{
				"BBF/202410120140/s994": {"BBF/202410120140/s994_f0"},
			},
		},
		{
			name: "test2",
			args: args{
				contents: []string{"BBF/202410120140/s994_f0", "BBF/202410120140/s994_f1", "BBF/202410120140/s994_f2", "BBF/202410120140/s994_f3"},
			},
			want: map[string][]string{
				"BBF/202410120140/s994": {"BBF/202410120140/s994_f0", "BBF/202410120140/s994_f1", "BBF/202410120140/s994_f2", "BBF/202410120140/s994_f3"},
			},
		},
		{
			name: "test3",
			args: args{
				contents: []string{"BBF/202410120140/s994_f0", "BBF/202410120140/s994_f1", "BBF/202410120150/s994_f0", "BBF/202410120150/s994_f1", "BBF/202410120130/s994_f0", "BBF/202410120120/s994_f0", "BBF/202410120140/s992_f0"},
			},
			want: map[string][]string{
				"BBF/202410120140/s994": {"BBF/202410120140/s994_f0", "BBF/202410120140/s994_f1"},
				"BBF/202410120150/s994": {"BBF/202410120150/s994_f0", "BBF/202410120150/s994_f1"},
				"BBF/202410120130/s994": {"BBF/202410120130/s994_f0"},
				"BBF/202410120120/s994": {"BBF/202410120120/s994_f0"},
				"BBF/202410120140/s992": {"BBF/202410120140/s992_f0"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMetas()
			m.addObject(tt.args.contents)
			toMap, _ := m.toMap()
			if !mapEqual(tt.want, toMap) {
				t.Errorf("addObject() = %v, want %v", toMap, tt.want)
			}
		})
	}
}

func TestMetas_combineMetas(t *testing.T) {
	type args struct {
		metas []byte
	}
	tests := []struct {
		name string
		args args
		want map[string][]string
	}{
		{
			name: "test0",
			args: args{
				metas: []byte(`{}`),
			},
			want: map[string][]string{},
		},
		{
			name: "test1",
			args: args{
				metas: []byte(`{"202410120140":{"0":[0,1,2,3]}}`),
			},
			want: map[string][]string{
				"BBF/202410120140/s0": {"BBF/202410120140/s0_f0"},
				"BBF/202410120140/s1": {"BBF/202410120140/s1_f0"},
				"BBF/202410120140/s2": {"BBF/202410120140/s2_f0"},
				"BBF/202410120140/s3": {"BBF/202410120140/s3_f0"},
			},
		},
		{
			name: "test2",
			args: args{
				metas: []byte(`{"202410120140":{"0":[0,1,2,3,4,5,6,7],"1":[4,5,6,7]}}`),
			},
			want: map[string][]string{
				"BBF/202410120140/s0": {"BBF/202410120140/s0_f0"},
				"BBF/202410120140/s1": {"BBF/202410120140/s1_f0"},
				"BBF/202410120140/s2": {"BBF/202410120140/s2_f0"},
				"BBF/202410120140/s3": {"BBF/202410120140/s3_f0"},
				"BBF/202410120140/s4": {"BBF/202410120140/s4_f0", "BBF/202410120140/s4_f1"},
				"BBF/202410120140/s5": {"BBF/202410120140/s5_f0", "BBF/202410120140/s5_f1"},
				"BBF/202410120140/s6": {"BBF/202410120140/s6_f0", "BBF/202410120140/s6_f1"},
				"BBF/202410120140/s7": {"BBF/202410120140/s7_f0", "BBF/202410120140/s7_f1"},
			},
		},
		{
			name: "test3",
			args: args{
				metas: []byte(`{"202410120140":{"0":[0,1],"1":[1]},"202410120150":{"0":[0],"1":[0]}, "202410120130":{"0":[0,1]}}`),
			},
			want: map[string][]string{
				"BBF/202410120140/s0": {"BBF/202410120140/s0_f0"},
				"BBF/202410120140/s1": {"BBF/202410120140/s1_f0", "BBF/202410120140/s1_f1"},
				"BBF/202410120150/s0": {"BBF/202410120150/s0_f0", "BBF/202410120150/s0_f1"},
				"BBF/202410120130/s0": {"BBF/202410120130/s0_f0"},
				"BBF/202410120130/s1": {"BBF/202410120130/s1_f0"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMetas()
			m.combineMetas(tt.args.metas)
			toMap, _ := m.toMap()
			if !mapEqual(tt.want, toMap) {
				t.Errorf("combineMetas() = %v, want %v", toMap, tt.want)
			}
		})
	}
}

var (
	// 2024-10-12 01:40:00
	time_202410120140 = time.Date(2024, 10, 12, 1, 40, 0, 0, time.UTC)

	// 2024-10-12 01:44:00
	time_202410120144 = time.Date(2024, 10, 12, 1, 44, 0, 0, time.UTC)

	// 2024-10-14 01:50:00
	time_202410140150 = time.Date(2024, 10, 14, 1, 50, 0, 0, time.UTC)

	// 2024-10-14 01:54:00
	time_202410140154 = time.Date(2024, 10, 14, 1, 54, 0, 0, time.UTC)
)

func Test_refreshTime_MetasPrefix(t1 *testing.T) {
	type fields struct {
		days    int
		hours   int
		minutes int
		now     time.Time
	}
	tests := []struct {
		name   string
		fields fields
		want   []*bucketOpt
	}{
		{
			name: "test0",
			fields: fields{
				days:    0,
				hours:   0,
				minutes: 0,
				now:     time.Now(),
			},
			want: []*bucketOpt{},
		},
		{
			name: "test1",
			fields: fields{
				days:    1,
				hours:   0,
				minutes: 0,
				now:     time.Now(),
			},
			want: []*bucketOpt{},
		},
		{
			name: "test2",
			fields: fields{
				days:    2,
				hours:   0,
				minutes: 0,
				now:     time_202410120140,
			},
			want: []*bucketOpt{
				{bucket: model.Time(1728610800000), prefix: "BBF/meta_20241011"},
			},
		},
		{
			name: "test3",
			fields: fields{
				days:    7,
				hours:   0,
				minutes: 0,
				now:     time_202410140150,
			},
			want: []*bucketOpt{
				{bucket: model.Time(1728784200000), prefix: "BBF/meta_20241013"},
				{bucket: model.Time(1728697800000), prefix: "BBF/meta_20241012"},
				{bucket: model.Time(1728611400000), prefix: "BBF/meta_20241011"},
				{bucket: model.Time(1728525000000), prefix: "BBF/meta_20241010"},
				{bucket: model.Time(1728438600000), prefix: "BBF/meta_20241009"},
				{bucket: model.Time(1728352200000), prefix: "BBF/meta_20241008"},
			},
		},
		{
			name: "test4",
			fields: fields{
				days:    7,
				hours:   0,
				minutes: 0,
				now:     time_202410140154,
			},
			want: []*bucketOpt{
				{bucket: model.Time(1728784200000), prefix: "BBF/meta_20241013"},
				{bucket: model.Time(1728697800000), prefix: "BBF/meta_20241012"},
				{bucket: model.Time(1728611400000), prefix: "BBF/meta_20241011"},
				{bucket: model.Time(1728525000000), prefix: "BBF/meta_20241010"},
				{bucket: model.Time(1728438600000), prefix: "BBF/meta_20241009"},
				{bucket: model.Time(1728352200000), prefix: "BBF/meta_20241008"},
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &refreshTime{
				days:      tt.fields.days,
				hours:     tt.fields.hours,
				minutes:   tt.fields.minutes,
				startTime: tt.fields.now,
			}
			t.parseToday()
			assert.Equalf(t1, tt.want, t.MetasPrefix(), "MetasPrefix()")
		})
	}
}

func Test_refreshTime_ListPrefix(t1 *testing.T) {
	type fields struct {
		days    int
		hours   int
		minutes int
		now     time.Time
	}
	tests := []struct {
		name   string
		fields fields
		want   []*bucketOpt
	}{
		{
			name: "test0",
			fields: fields{
				days:    0,
				hours:   0,
				minutes: 0,
				now:     time.Now(),
			},
			want: []*bucketOpt{},
		},
		{
			name: "test1",
			fields: fields{
				days:    1,
				hours:   0,
				minutes: 0,
				now:     time_202410120140,
			},
			want: []*bucketOpt{
				{bucket: model.Time(1728691200000), prefix: "BBF/202410120000"},
				{bucket: model.Time(1728691800000), prefix: "BBF/202410120010"},
				{bucket: model.Time(1728692400000), prefix: "BBF/202410120020"},
				{bucket: model.Time(1728693000000), prefix: "BBF/202410120030"},
				{bucket: model.Time(1728693600000), prefix: "BBF/202410120040"},
				{bucket: model.Time(1728694200000), prefix: "BBF/202410120050"},
				{bucket: model.Time(1728694800000), prefix: "BBF/202410120100"},
				{bucket: model.Time(1728695400000), prefix: "BBF/202410120110"},
				{bucket: model.Time(1728696000000), prefix: "BBF/202410120120"},
				{bucket: model.Time(1728696600000), prefix: "BBF/202410120130"},
			},
		},
		{
			name: "test2",
			fields: fields{
				days:    1,
				hours:   0,
				minutes: 0,
				now:     time_202410120144,
			},
			want: []*bucketOpt{
				{bucket: model.Time(1728691200000), prefix: "BBF/202410120000"},
				{bucket: model.Time(1728691800000), prefix: "BBF/202410120010"},
				{bucket: model.Time(1728692400000), prefix: "BBF/202410120020"},
				{bucket: model.Time(1728693000000), prefix: "BBF/202410120030"},
				{bucket: model.Time(1728693600000), prefix: "BBF/202410120040"},
				{bucket: model.Time(1728694200000), prefix: "BBF/202410120050"},
				{bucket: model.Time(1728694800000), prefix: "BBF/202410120100"},
				{bucket: model.Time(1728695400000), prefix: "BBF/202410120110"},
				{bucket: model.Time(1728696000000), prefix: "BBF/202410120120"},
				{bucket: model.Time(1728696600000), prefix: "BBF/202410120130"},
				{bucket: model.Time(1728697200000), prefix: "BBF/202410120140"},
			},
		},
		{
			name: "test3",
			fields: fields{
				days:    0,
				hours:   1,
				minutes: 0,
				now:     time_202410120140,
			},
			want: []*bucketOpt{
				{bucket: model.Time(1728693600000), prefix: "BBF/202410120040"},
				{bucket: model.Time(1728694200000), prefix: "BBF/202410120050"},
				{bucket: model.Time(1728694800000), prefix: "BBF/202410120100"},
				{bucket: model.Time(1728695400000), prefix: "BBF/202410120110"},
				{bucket: model.Time(1728696000000), prefix: "BBF/202410120120"},
				{bucket: model.Time(1728696600000), prefix: "BBF/202410120130"},
			},
		},
		{
			name: "test4",
			fields: fields{
				days:    0,
				hours:   0,
				minutes: 10,
				now:     time_202410120140,
			},
			want: []*bucketOpt{
				{bucket: model.Time(1728696600000), prefix: "BBF/202410120130"},
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &refreshTime{
				days:      tt.fields.days,
				hours:     tt.fields.hours,
				minutes:   tt.fields.minutes,
				startTime: tt.fields.now,
			}
			t.parseToday()
			opts := t.ListPrefix()
			assert.Equalf(t1, tt.want, opts, "ListPrefix()")
		})
	}
}
