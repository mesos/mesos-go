package mesos_test

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/resourcefilters"
	rez "github.com/mesos/mesos-go/api/v1/lib/resources"
	. "github.com/mesos/mesos-go/api/v1/lib/resourcetest"
)

func TestResources_PrecisionRounding(t *testing.T) {
	var (
		cpu = Resources(Resource(Name("cpus"), ValueScalar(1.5015)))
		r1  = cpu.Plus(cpu...).Plus(cpu...).Minus(cpu...).Minus(cpu...)
	)
	if !rez.Equivalent(cpu, r1) {
		t.Fatalf("expected %v instead of %v", cpu, r1)
	}
	actual, ok := rez.CPUs(r1...)
	if !(ok && actual == 1.502) {
		t.Fatalf("expected 1.502 cpus instead of %v", actual)
	}
}

func TestResources_PrecisionLost(t *testing.T) {
	var (
		cpu = Resources(Resource(Name("cpus"), ValueScalar(1.5011)))
		r1  = cpu.Plus(cpu...).Plus(cpu...).Minus(cpu...).Minus(cpu...)
	)
	if !rez.Equivalent(cpu, r1) {
		t.Fatalf("expected %v instead of %v", cpu, r1)
	}
	actual, ok := rez.CPUs(r1...)
	if !(ok && actual == 1.501) {
		t.Fatalf("expected 1.501 cpus instead of %v", actual)
	}
}

func TestResources_PrecisionManyConsecutiveOps(t *testing.T) {
	var (
		start     = Resources(Resource(Name("cpus"), ValueScalar(1.001)))
		increment = start.Clone()
		current   = start.Clone()
	)
	for i := 0; i < 100000; i++ {
		current.Add(increment...)
	}
	for i := 0; i < 100000; i++ {
		current.Subtract(increment...)
	}
	if !rez.Equivalent(start, current) {
		t.Fatalf("expected start %v == current %v", start, current)
	}
}

func TestResources_PrecisionManyOps(t *testing.T) {
	var (
		start   = Resources(Resource(Name("cpus"), ValueScalar(1.001)))
		current = start.Clone()
		next    mesos.Resources
	)
	for i := 0; i < 2500; i++ {
		next = current.Plus(current...).Plus(current...).Minus(current...).Minus(current...)
		actual, ok := rez.CPUs(next...)
		if !(ok && actual == 1.001) {
			t.Fatalf("expected 1.001 cpus instead of %v", next)
		}
		if !rez.Equivalent(current, next) {
			t.Fatalf("expected current %v == next %v", current, next)
		}
		if !rez.Equivalent(start, next) {
			t.Fatalf("expected start %v == next %v", start, next)
		}
	}
}

func TestResources_PrecisionSimple(t *testing.T) {
	var (
		cpu  = Resources(Resource(Name("cpus"), ValueScalar(1.001)))
		zero = mesos.Resources{Resource(Name("cpus"), ValueScalar(0))} // don't validate
	)
	actual, ok := rez.CPUs(cpu...)
	if !(ok && actual == 1.001) {
		t.Errorf("expected 1.001 instead of %f", actual)
	}
	if x := cpu.Plus(zero...); !rez.Equivalent(x, cpu) {
		t.Errorf("adding zero failed, expected '%v' instead of '%v'", cpu, x)
	}
	if y := cpu.Minus(zero...); !rez.Equivalent(y, cpu) {
		t.Errorf("subtracting zero failed, expected '%v' instead of '%v'", cpu, y)
	}
}

func TestResource_RevocableResources(t *testing.T) {
	rs := mesos.Resources{
		Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable()),
		Resource(Name("cpus"), ValueScalar(1), Role("*")),
	}
	for i, tc := range []struct {
		r1, wants mesos.Resources
	}{
		{Resources(rs[0]), Resources(rs[0])},
		{Resources(rs[1]), Resources()},
		{Resources(rs[0], rs[1]), Resources(rs[0])},
	} {
		x := resourcefilters.Select(resourcefilters.New(resourcefilters.Revocable), tc.r1...)
		if !rez.Equivalent(tc.wants, x) {
			t.Errorf("test case %d failed: expected %v instead of %v", i, tc.wants, x)
		}
	}
}

func TestResources_PersistentVolumes(t *testing.T) {
	var (
		rs = Resources(
			Resource(Name("cpus"), ValueScalar(1)),
			Resource(Name("mem"), ValueScalar(512)),
			Resource(Name("disk"), ValueScalar(1000)),
		)
		disk = mesos.Resources{
			Resource(Name("disk"), ValueScalar(10), Role("role1"), Disk("1", "path")),
			Resource(Name("disk"), ValueScalar(20), Role("role2"), Disk("", "")),
		}
	)
	rs.Add(disk...)
	pv := resourcefilters.Select(resourcefilters.New(resourcefilters.PersistentVolumes), rs...)
	if !rez.Equivalent(Resources(disk[0]), pv) {
		t.Fatalf("expected %v instead of %v", Resources(disk[0]), pv)
	}
}

func TestResource_IsEmpty(t *testing.T) {
	for i, tc := range []struct {
		r     mesos.Resource
		wants bool
	}{
		{Resource(), true},
		{Resource(ValueScalar(0)), true},
		{Resource(ValueSet()), true},
		{Resource(ValueSet([]string{}...)), true},
		{Resource(ValueSet()), true},
		{Resource(ValueSet("")), false},
		{Resource(ValueRange()), true},
		{Resource(ValueRange(Span(0, 0))), false},
	} {
		actual := tc.r.IsEmpty()
		Expect(t, tc.wants == actual, "test case %d failed: wants (%v) != actual (%v)", i, tc.wants, actual)
	}
}

func TestResources_Minus(t *testing.T) {
	disks := mesos.Resources{
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("", "path")),
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("", "")),
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("1", "path")),
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("2", "path")),
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("2", "path2")),
	}
	for i, tc := range []struct {
		r1, r2      mesos.Resources
		wants       mesos.Resources
		wantsCPU    float64
		wantsMemory uint64
	}{
		{r1: nil, r2: nil, wants: nil},
		{r1: Resources(), r2: Resources(), wants: Resources()},
		// simple scalars, same roles for everything
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("*")),
				Resource(Name("mem"), ValueScalar(4096), Role("*")),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(0.5), Role("*")),
				Resource(Name("mem"), ValueScalar(1024), Role("*")),
			),
			wants: Resources(
				Resource(Name("cpus"), ValueScalar(49.5), Role("*")),
				Resource(Name("mem"), ValueScalar(3072), Role("*")),
			),
			wantsCPU:    49.5,
			wantsMemory: 3072,
		},
		// multi-role, scalar subtraction
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(5), Role("role1")),
				Resource(Name("cpus"), ValueScalar(3), Role("role2")),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(1), Role("role1")),
			),
			wants: Resources(
				Resource(Name("cpus"), ValueScalar(4), Role("role1")),
				Resource(Name("cpus"), ValueScalar(3), Role("role2")),
			),
			wantsCPU: 7,
		},
		// simple ranges, same roles, lower-edge overlap
		{
			r1: Resources(
				Resource(Name("ports"), ValueRange(Span(20000, 40000)), Role("*")),
			),
			r2: Resources(
				Resource(Name("ports"), ValueRange(Span(10000, 20000), Span(30000, 50000)), Role("*")),
			),
			wants: Resources(
				Resource(Name("ports"), ValueRange(Span(20001, 29999)), Role("*")),
			),
		},
		// simple ranges, same roles, single port/lower-edge
		{
			r1: Resources(
				Resource(Name("ports"), ValueRange(Span(50000, 60000)), Role("*")),
			),
			r2: Resources(
				Resource(Name("ports"), ValueRange(Span(50000, 50000)), Role("*")),
			),
			wants: Resources(
				Resource(Name("ports"), ValueRange(Span(50001, 60000)), Role("*")),
			),
		},
		// simple ranges, same roles, multi port/lower-edge
		{
			r1: Resources(
				Resource(Name("ports"), ValueRange(Span(50000, 60000)), Role("*")),
			),
			r2: Resources(
				Resource(Name("ports"), ValueRange(Span(50000, 50001)), Role("*")),
			),
			wants: Resources(
				Resource(Name("ports"), ValueRange(Span(50002, 60000)), Role("*")),
			),
		},
		// simple ranges, same roles, identical overlap
		{
			r1: Resources(
				Resource(Name("ports"), ValueRange(Span(50000, 60000)), Role("*")),
			),
			r2: Resources(
				Resource(Name("ports"), ValueRange(Span(50000, 60000)), Role("*")),
			),
			wants: Resources(),
		},
		// multiple ranges, same roles, swiss cheese
		{
			r1: Resources(
				Resource(Name("ports"), ValueRange(Span(1, 10), Span(20, 30), Span(40, 50)), Role("*")),
			),
			r2: Resources(
				Resource(Name("ports"), ValueRange(Span(2, 9), Span(15, 45), Span(48, 50)), Role("*")),
			),
			wants: Resources(
				Resource(Name("ports"), ValueRange(Span(1, 1), Span(10, 10), Span(46, 47)), Role("*")),
			),
		},
		// multiple ranges, same roles, no overlap
		{
			r1: Resources(
				Resource(Name("ports"), ValueRange(Span(1, 10)), Role("*")),
			),
			r2: Resources(
				Resource(Name("ports"), ValueRange(Span(11, 20)), Role("*")),
			),
			wants: Resources(
				Resource(Name("ports"), ValueRange(Span(1, 10)), Role("*")),
			),
		},
		// simple set, same roles
		{
			r1: Resources(
				Resource(Name("disks"), ValueSet("sda1", "sda2", "sda3", "sda4"), Role("*")),
			),
			r2: Resources(
				Resource(Name("disks"), ValueSet("sda2", "sda3", "sda4"), Role("*")),
			),
			wants: Resources(
				Resource(Name("disks"), ValueSet("sda1"), Role("*")),
			),
		},
		{r1: Resources(disks[0]), r2: Resources(disks[1]), wants: Resources()},
		{r1: Resources(disks[2]), r2: Resources(disks[3]), wants: Resources(disks[2])},
		{r1: Resources(disks[2]), r2: Resources(disks[2]), wants: Resources()},
		{r1: Resources(disks[3]), r2: Resources(disks[4]), wants: Resources()},
		// revocables
		{
			r1:    Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable())),
			r2:    Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable())),
			wants: Resources(),
		},
		{ // revocable - non-revocable is a noop
			r1:       Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable())),
			r2:       Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"))),
			wants:    Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable())),
			wantsCPU: 1,
		},
		// reserved
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(8), Role("role")),
				Resource(Name("cpus"), ValueScalar(8), Role("role"), Reservation(ReservedBy("principal"))),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(2), Role("role")),
				Resource(Name("cpus"), ValueScalar(4), Role("role"), Reservation(ReservedBy("principal"))),
			),
			wants: Resources(
				Resource(Name("cpus"), ValueScalar(6), Role("role")),
				Resource(Name("cpus"), ValueScalar(4), Role("role"), Reservation(ReservedBy("principal"))),
			),
			wantsCPU: 10,
		},
	} {
		backup := tc.r1.Clone()

		// Minus preserves the left operand
		actual := tc.r1.Minus(tc.r2...)
		if !rez.Equivalent(tc.wants, actual) {
			t.Errorf("test case %d failed: wants (%v) != actual (%v)", i, tc.wants, actual)
		}
		if !rez.Equivalent(backup, tc.r1) {
			t.Errorf("test case %d failed: backup (%v) != r1 (%v)", i, backup, tc.r1)
		}

		// SubtractAll mutates the left operand
		tc.r1.Subtract(tc.r2...)
		if !rez.Equivalent(tc.wants, tc.r1) {
			t.Errorf("test case %d failed: wants (%v) != r1 (%v)", i, tc.wants, tc.r1)
		}

		cpus, ok := rez.CPUs(tc.r1...)
		if !ok && tc.wantsCPU > 0 {
			t.Errorf("test case %d failed: failed to obtain total CPU resources", i)
		} else if cpus != tc.wantsCPU {
			t.Errorf("test case %d failed: wants cpu (%v) != r1 cpu (%v)", i, tc.wantsCPU, cpus)
		}

		mem, ok := rez.Memory(tc.r1...)
		if !ok && tc.wantsMemory > 0 {
			t.Errorf("test case %d failed: failed to obtain total memory resources", i)
		} else if mem != tc.wantsMemory {
			t.Errorf("test case %d failed: wants mem (%v) != r1 mem (%v)", i, tc.wantsMemory, mem)
		}

		t.Logf("substracting tc.r1 from itself\n")
		tc.r1.Subtract(tc.r1...)
		if len(tc.r1) > 0 {
			t.Errorf("test case %d failed: r1 is not empty (%v)", i, tc.r1)
		}
	}
}

func TestResources_Plus(t *testing.T) {
	disks := mesos.Resources{
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("", "path")),
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("", "")),
		Resource(Name("disk"), ValueScalar(20), Role("role"), Disk("", "path")),
	}
	for i, tc := range []struct {
		r1, r2      mesos.Resources
		wants       mesos.Resources
		wantsCPU    float64
		wantsMemory uint64
	}{
		{r1: Resources(disks[0]), r2: Resources(disks[1]), wants: Resources(disks[2])},
		{r1: nil, r2: nil, wants: nil},
		{r1: Resources(), r2: Resources(), wants: Resources()},
		// simple scalars, same roles for everything
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(1), Role("*")),
				Resource(Name("mem"), ValueScalar(5), Role("*")),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(2), Role("*")),
				Resource(Name("mem"), ValueScalar(10), Role("*")),
			),
			wants: Resources(
				Resource(Name("cpus"), ValueScalar(3), Role("*")),
				Resource(Name("mem"), ValueScalar(15), Role("*")),
			),
			wantsCPU:    3,
			wantsMemory: 15,
		},
		// simple scalars, differing roles
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(1), Role("role1")),
				Resource(Name("cpus"), ValueScalar(3), Role("role2")),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(5), Role("role1")),
			),
			wants: Resources(
				Resource(Name("cpus"), ValueScalar(6), Role("role1")),
				Resource(Name("cpus"), ValueScalar(3), Role("role2")),
			),
			wantsCPU: 9,
		},
		// ranges addition yields continuous range
		{
			r1: Resources(
				Resource(Name("ports"), ValueRange(Span(20000, 40000)), Role("*")),
			),
			r2: Resources(
				Resource(Name("ports"), ValueRange(Span(30000, 50000), Span(10000, 20000)), Role("*")),
			),
			wants: Resources(
				Resource(Name("ports"), ValueRange(Span(10000, 50000)), Role("*")),
			),
		},
		// ranges addition yields a split set of ranges
		{
			r1: Resources(
				Resource(Name("ports"), ValueRange(Span(1, 10), Span(5, 30), Span(50, 60)), Role("*")),
				Resource(Name("ports"), ValueRange(Span(1, 65), Span(70, 80)), Role("*")),
			),
			wants: Resources(
				Resource(Name("ports"), ValueRange(Span(1, 65), Span(70, 80)), Role("*")),
			),
		},
		// ranges addition (composite) yields a continuous range
		{
			r1: Resources(
				Resource(Name("ports"), ValueRange(Span(1, 2)), Role("*")),
				Resource(Name("ports"), ValueRange(Span(3, 4)), Role("*")),
			),
			r2: Resources(
				Resource(Name("ports"), ValueRange(Span(7, 8)), Role("*")),
				Resource(Name("ports"), ValueRange(Span(5, 6)), Role("*")),
			),
			wants: Resources(
				Resource(Name("ports"), ValueRange(Span(1, 8)), Role("*")),
			),
		},
		// ranges addition yields a split set of ranges
		{
			r1: Resources(
				Resource(Name("ports"), ValueRange(Span(1, 4), Span(9, 10), Span(20, 22), Span(26, 30)), Role("*")),
			),
			r2: Resources(
				Resource(Name("ports"), ValueRange(Span(5, 8), Span(23, 25)), Role("*")),
			),
			wants: Resources(
				Resource(Name("ports"), ValueRange(Span(1, 10), Span(20, 30)), Role("*")),
			),
		},
		// set addition
		{
			r1: Resources(
				Resource(Name("disks"), ValueSet("sda1", "sda2", "sda3"), Role("*")),
			),
			r2: Resources(
				Resource(Name("disks"), ValueSet("sda1", "sda2", "sda3", "sda4"), Role("*")),
			),
			wants: Resources(
				Resource(Name("disks"), ValueSet("sda4", "sda2", "sda1", "sda3"), Role("*")),
			),
		},
		// revocables
		{
			r1:       Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable())),
			r2:       Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable())),
			wants:    Resources(Resource(Name("cpus"), ValueScalar(2), Role("*"), Revocable())),
			wantsCPU: 2,
		},
		// statically reserved
		{
			r1:       Resources(Resource(Name("cpus"), ValueScalar(8), Role("role"))),
			r2:       Resources(Resource(Name("cpus"), ValueScalar(4), Role("role"))),
			wants:    Resources(Resource(Name("cpus"), ValueScalar(12), Role("role"))),
			wantsCPU: 12,
		},
		// dynamically reserved
		{
			r1:       Resources(Resource(Name("cpus"), ValueScalar(8), Role("role"), Reservation(ReservedBy("principal")))),
			r2:       Resources(Resource(Name("cpus"), ValueScalar(4), Role("role"), Reservation(ReservedBy("principal")))),
			wants:    Resources(Resource(Name("cpus"), ValueScalar(12), Role("role"), Reservation(ReservedBy("principal")))),
			wantsCPU: 12,
		},
	} {
		backup := tc.r1.Clone()

		// Plus preserves the left operand
		actual := tc.r1.Plus(tc.r2...)
		if !rez.Equivalent(tc.wants, actual) {
			t.Errorf("test case %d failed: wants (%v) != actual (%v)", i, tc.wants, actual)
		}
		if !rez.Equivalent(backup, tc.r1) {
			t.Errorf("test case %d failed: backup (%v) != r1 (%v)", i, backup, tc.r1)
		}

		// Add mutates the left operand
		tc.r1.Add(tc.r2...)
		if !rez.Equivalent(tc.wants, tc.r1) {
			t.Errorf("test case %d failed: wants (%v) != r1 (%v)", i, tc.wants, tc.r1)
		}

		cpus, ok := rez.CPUs(tc.r1...)
		if !ok && tc.wantsCPU > 0 {
			t.Errorf("test case %d failed: failed to obtain total CPU resources", i)
		} else if cpus != tc.wantsCPU {
			t.Errorf("test case %d failed: wants cpu (%v) != r1 cpu (%v)", i, tc.wantsCPU, cpus)
		}

		mem, ok := rez.Memory(tc.r1...)
		if !ok && tc.wantsMemory > 0 {
			t.Errorf("test case %d failed: failed to obtain total memory resources", i)
		} else if mem != tc.wantsMemory {
			t.Errorf("test case %d failed: wants mem (%v) != r1 mem (%v)", i, tc.wantsMemory, mem)
		}
	}
}

func TestDiskTypeIdentityProfile(t *testing.T) {
	var (
		id      = "id"
		profile = "profile"
	)
	for ti, tc := range []struct {
		t          mesos.Resource_DiskInfo_Source_Type
		hasID      bool
		hasProfile bool
	}{
		{t: mesos.Resource_DiskInfo_Source_RAW, hasID: false, hasProfile: false},
		{t: mesos.Resource_DiskInfo_Source_RAW, hasID: false, hasProfile: true},
		{t: mesos.Resource_DiskInfo_Source_RAW, hasID: true, hasProfile: false},
		{t: mesos.Resource_DiskInfo_Source_RAW, hasID: true, hasProfile: true},
		{t: mesos.Resource_DiskInfo_Source_BLOCK, hasID: false, hasProfile: false},
		{t: mesos.Resource_DiskInfo_Source_BLOCK, hasID: false, hasProfile: true},
		{t: mesos.Resource_DiskInfo_Source_BLOCK, hasID: true, hasProfile: false},
		{t: mesos.Resource_DiskInfo_Source_BLOCK, hasID: true, hasProfile: true},
		{t: mesos.Resource_DiskInfo_Source_MOUNT, hasID: false, hasProfile: false},
		{t: mesos.Resource_DiskInfo_Source_MOUNT, hasID: false, hasProfile: true},
		{t: mesos.Resource_DiskInfo_Source_MOUNT, hasID: true, hasProfile: false},
		{t: mesos.Resource_DiskInfo_Source_MOUNT, hasID: true, hasProfile: true},
		{t: mesos.Resource_DiskInfo_Source_PATH, hasID: false, hasProfile: false},
		{t: mesos.Resource_DiskInfo_Source_PATH, hasID: false, hasProfile: true},
		{t: mesos.Resource_DiskInfo_Source_PATH, hasID: true, hasProfile: false},
		{t: mesos.Resource_DiskInfo_Source_PATH, hasID: true, hasProfile: true},
	} {
		t.Run(fmt.Sprintf("test case %d", ti), func(t *testing.T) {
			disk1 := Resource(Name("disk"), ValueScalar(1), Role("*"), DiskWithSource("", "", "", tc.t))
			if tc.hasID {
				disk1.GetDisk().GetSource().ID = &id
			}
			if tc.hasProfile {
				disk1.GetDisk().GetSource().Profile = &profile
			}
			r1 := Resources(disk1)
			if !rez.Contains(r1, disk1) {
				t.Errorf("expected %v to contain %v", r1, disk1)
			}

			disk2 := Resource(Name("disk"), ValueScalar(2), Role("*"), DiskWithSource("", "", "", tc.t))
			disk2.Disk.Source = proto.Clone(disk1.Disk.Source).(*mesos.Resource_DiskInfo_Source)
			r2 := Resources(disk2)

			r3 := r1.Plus(r1...)
			sz := len(r3)

			switch tc.t {
			case mesos.Resource_DiskInfo_Source_RAW:
				if tc.hasID {
					// `RAW` resources with source identity cannot be added or split.
					assertf(t, !rez.ContainsAll(r2, r1), "expected %v to NOT contain %v", r2, r1)
					assertf(t, !rez.Equivalent(r2, r3), "expected r2 != r1+r1")
					assertf(t, sz == 2, "expected size(r1+r1) == 2 instead of %d", sz)
				} else {
					// `RAW` resources without source identity can be added and split.
					assertf(t, rez.ContainsAll(r2, r1), "expected %v to contain %v", r2, r1)
					assertf(t, rez.Equivalent(r2, r3), "expected r2 == r1+r1")
					assertf(t, sz == 1, "expected size(r1+r1) == 1 instead of %d", sz)
				}

			case mesos.Resource_DiskInfo_Source_BLOCK,
				mesos.Resource_DiskInfo_Source_MOUNT:
				// `BLOCK` or `MOUNT` resources cannot be added or split,
				// regardless of identity.
				assertf(t, !rez.ContainsAll(r2, r1), "expected %v to NOT contain %v", r2, r1)
				assertf(t, !rez.Equivalent(r2, r3), "expected r2 != r1+r1")
				assertf(t, sz == 2, "expected size(r1+r1) == 2 instead of %d", sz)

			case mesos.Resource_DiskInfo_Source_PATH:
				// `PATH` resources can be added and split, regardless of identity.
				assertf(t, rez.ContainsAll(r2, r1), "expected %v to contain %v", r2, r1)
				assertf(t, rez.Equivalent(r2, r3), "expected r2 == r1+r1")
				assertf(t, sz == 1, "expected size(r1+r1) == 1 instead of %d", sz)

			case mesos.Resource_DiskInfo_Source_UNKNOWN:
				t.Fatalf("unexpected disk source type: UNKNOWN")
			}
		})
	}
}

func assertf(t *testing.T, cond bool, msg string, args ...interface{}) {
	if !cond {
		t.Errorf(msg, args...)
	}
}
