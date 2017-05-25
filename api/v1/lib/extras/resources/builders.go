package resources

import (
	"github.com/mesos/mesos-go/api/v1/lib"
)

type (
	// Builder simplifies construction of Resource objects
	Builder struct{ mesos.Resource }
	// RangeBuilder simplifies construction of Range objects
	RangeBuilder struct{ mesos.Ranges }
)

func CPUs(value float64) *Builder {
	return Build().Name("cpus").Scalar(value)
}

func Memory(value float64) *Builder {
	return Build().Name("mem").Scalar(value)
}

func Disk(value float64) *Builder {
	return Build().Name("disk").Scalar(value)
}

func GPUs(value uint) *Builder {
	return Build().Name("gpus").Scalar(float64(value))
}

func BuildRanges() *RangeBuilder {
	return &RangeBuilder{Ranges: mesos.Ranges(nil)}
}

// Span is a functional option for Ranges, defines the begin and end points of a
// continuous span within a range
func (rb *RangeBuilder) Span(bp, ep uint64) *RangeBuilder {
	rb.Ranges = append(rb.Ranges, mesos.Value_Range{Begin: bp, End: ep})
	return rb
}

func Build() *Builder {
	return &Builder{}
}
func (rb *Builder) Name(name string) *Builder {
	rb.Resource.Name = name
	return rb
}
func (rb *Builder) Role(role string) *Builder {
	rb.Resource.Role = &role
	return rb
}
func (rb *Builder) Scalar(x float64) *Builder {
	rb.Resource.Type = mesos.SCALAR.Enum()
	rb.Resource.Scalar = &mesos.Value_Scalar{Value: x}
	return rb
}
func (rb *Builder) Set(x ...string) *Builder {
	rb.Resource.Type = mesos.SET.Enum()
	rb.Resource.Set = &mesos.Value_Set{Item: x}
	return rb
}
func (rb *Builder) Ranges(rs mesos.Ranges) *Builder {
	rb.Resource.Type = mesos.RANGES.Enum()
	rb.Resource.Ranges = rb.Resource.Ranges.Add(&mesos.Value_Ranges{Range: rs})
	return rb
}
func (rb *Builder) Disk(persistenceID, containerPath string) *Builder {
	rb.Resource.Disk = &mesos.Resource_DiskInfo{}
	if containerPath != "" {
		rb.Resource.Disk.Volume = &mesos.Volume{ContainerPath: containerPath}
	}
	if persistenceID != "" {
		rb.Resource.Disk.Persistence = &mesos.Resource_DiskInfo_Persistence{ID: persistenceID}
	}
	return rb
}
func (rb *Builder) Revocable() *Builder {
	rb.Resource.Revocable = &mesos.Resource_RevocableInfo{}
	return rb
}
