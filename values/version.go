package values

import (
	"math"
)

type Version struct {
	Val string

	Deleted   bool
	StartTime uint64
	EndTime   uint64
	Next      *Version
}

func NewVersion(val string) *Version {
	return &Version{
		Val:       val,
		Deleted:   false,
		StartTime: math.MaxUint64,
		EndTime:   math.MaxUint64,
		Next:      nil,
	}
}

func (v *Version) IsVisible(ts uint64) bool {
	return v.StartTime <= ts && ts < v.EndTime
}

func (v *Version) Install(commitID uint64) {
	v.StartTime = commitID
	v.EndTime = math.MaxUint64
	if v.Next != nil {
		v.Next.EndTime = commitID
	}
}
