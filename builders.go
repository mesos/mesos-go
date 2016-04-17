package mesos

type (
	// ResourceBuilder simplifies construction of Resource objects
	ResourceBuilder struct{ *Resource }
	// RangeBuilder simplifies construction of Range objects
	RangeBuilder struct{ Ranges }
)

func BuildRanges() RangeBuilder {
	return RangeBuilder{Ranges: Ranges(nil)}
}

// Span is a functional option for Ranges, defines the begin and end points of a
// continuous span within a range
func (rb RangeBuilder) Span(bp, ep uint64) RangeBuilder {
	rb.Ranges = append(rb.Ranges, Value_Range{Begin: bp, End: ep})
	return rb
}

func BuildResource() ResourceBuilder {
	return ResourceBuilder{&Resource{}}
}
func (rb ResourceBuilder) Name(name string) ResourceBuilder {
	rb.Resource.Name = name
	return rb
}
func (rb ResourceBuilder) Role(role string) ResourceBuilder {
	rb.Resource.Role = &role
	return rb
}
func (rb ResourceBuilder) Scalar(x float64) ResourceBuilder {
	rb.Resource.Type = SCALAR.Enum()
	rb.Resource.Scalar = &Value_Scalar{Value: x}
	return rb
}
func (rb ResourceBuilder) Set(x ...string) ResourceBuilder {
	rb.Resource.Type = SET.Enum()
	rb.Resource.Set = &Value_Set{Item: x}
	return rb
}
func (rb ResourceBuilder) Ranges(rs Ranges) ResourceBuilder {
	rb.Resource.Type = RANGES.Enum()
	rb.Resource.Ranges = rb.Resource.Ranges.Add(&Value_Ranges{Range: rs})
	return rb
}
func (rb ResourceBuilder) Disk(persistenceID, containerPath string) ResourceBuilder {
	rb.Resource.Disk = &Resource_DiskInfo{}
	if containerPath != "" {
		rb.Resource.Disk.Volume = &Volume{ContainerPath: containerPath}
	}
	if persistenceID != "" {
		rb.Resource.Disk.Persistence = &Resource_DiskInfo_Persistence{ID: persistenceID}
	}
	return rb
}
func (rb ResourceBuilder) Revocable() ResourceBuilder {
	rb.Resource.Revocable = &Resource_RevocableInfo{}
	return rb
}
