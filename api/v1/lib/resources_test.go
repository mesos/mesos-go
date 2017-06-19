package mesos_test

import (
	"testing"

	"github.com/mesos/mesos-go/api/v1/lib"
	rez "github.com/mesos/mesos-go/api/v1/lib/extras/resources"
	"github.com/mesos/mesos-go/api/v1/lib/resourcefilters"
	. "github.com/mesos/mesos-go/api/v1/lib/resourcetest"
)

func TestResources_PrecisionRounding(t *testing.T) {
	var (
		cpu = Resources(Resource(Name("cpus"), ValueScalar(1.5015)))
		r1  = cpu.Plus(cpu...).Plus(cpu...).Minus(cpu...).Minus(cpu...)
	)
	if !cpu.Equivalent(r1) {
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
	if !cpu.Equivalent(r1) {
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
	if !start.Equivalent(current) {
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
		if !current.Equivalent(next) {
			t.Fatalf("expected current %v == next %v", current, next)
		}
		if !start.Equivalent(next) {
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
	if x := cpu.Plus(zero...); !x.Equivalent(cpu) {
		t.Errorf("adding zero failed, expected '%v' instead of '%v'", cpu, x)
	}
	if y := cpu.Minus(zero...); !y.Equivalent(cpu) {
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
		if !tc.wants.Equivalent(x) {
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
	if !Resources(disk[0]).Equivalent(pv) {
		t.Fatalf("expected %v instead of %v", Resources(disk[0]), pv)
	}
}

func TestResources_Validation(t *testing.T) {
	// don't use Resources(...) because that implicitly validates and skips invalid resources
	rs := mesos.Resources{
		Resource(Name("cpus"), ValueScalar(2), Role("*"), Disk("1", "path")),
	}
	err := rs.Validate()
	if !mesos.IsResourceError(err) {
		t.Fatalf("expected error because cpu resources can't contain disk info")
	}

	err = mesos.Resources{Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("1", "path"))}.Validate()
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}

	err = mesos.Resources{Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("", "path"))}.Validate()
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}

	// reserved resources

	// unreserved:
	err = mesos.Resources{Resource(Name("cpus"), ValueScalar(8), Role("*"))}.Validate()
	if err != nil {
		t.Fatalf("unexpected error validating unreserved resource: %+v", err)
	}

	// statically role reserved:
	err = mesos.Resources{Resource(Name("cpus"), ValueScalar(8), Role("role"))}.Validate()
	if err != nil {
		t.Fatalf("unexpected error validating statically role reserved resource: %+v", err)
	}

	// dynamically role reserved:
	err = mesos.Resources{Resource(Name("cpus"), ValueScalar(8), Role("role"), Reservation(ReservedBy("principal2")))}.Validate()
	if err != nil {
		t.Fatalf("unexpected error validating dynamically role reserved resource: %+v", err)
	}

	// invalid
	err = mesos.Resources{Resource(Name("cpus"), ValueScalar(8), Role("*"), Reservation(ReservedBy("principal1")))}.Validate()
	if err == nil {
		t.Fatalf("expected error for invalid reserved resource")
	}
}

func TestResources_Flatten(t *testing.T) {
	for i, tc := range []struct {
		r1, wants mesos.Resources
	}{
		{nil, nil},
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(1), Role("role1")),
				Resource(Name("cpus"), ValueScalar(2), Role("role2")),
				Resource(Name("mem"), ValueScalar(5), Role("role1")),
			),
			wants: Resources(
				Resource(Name("cpus"), ValueScalar(3)),
				Resource(Name("mem"), ValueScalar(5)),
			),
		},
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(3), Role("role1")),
				Resource(Name("mem"), ValueScalar(15), Role("role1")),
			),
			wants: Resources(
				Resource(Name("cpus"), ValueScalar(3), Role("*")),
				Resource(Name("mem"), ValueScalar(15), Role("*")),
			),
		},
	} {
		r := tc.r1.Flatten()
		Expect(t, r.Equivalent(tc.wants), "test case %d failed: expected %+v instead of %+v", i, tc.wants, r)
	}
}

func TestResources_Equivalent(t *testing.T) {
	disks := mesos.Resources{
		Resource(Name("disk"), ValueScalar(10), Role("*"), Disk("", "")),
		Resource(Name("disk"), ValueScalar(10), Role("*"), Disk("", "path1")),
		Resource(Name("disk"), ValueScalar(10), Role("*"), Disk("", "path2")),
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("", "path2")),
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("1", "path1")),
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("1", "path2")),
		Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("2", "path2")),
	}
	for i, tc := range []struct {
		r1, r2 mesos.Resources
		wants  bool
	}{
		{r1: nil, r2: nil, wants: true},
		{ // 1
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("*")),
				Resource(Name("mem"), ValueScalar(4096), Role("*")),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("*")),
				Resource(Name("mem"), ValueScalar(4096), Role("*")),
			),
			wants: true,
		},
		{ // 2
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("role1")),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("role2")),
			),
			wants: false,
		},
		{ // 3
			r1:    Resources(Resource(Name("ports"), ValueRange(Span(20, 40)), Role("*"))),
			r2:    Resources(Resource(Name("ports"), ValueRange(Span(20, 30), Span(31, 39), Span(40, 40)), Role("*"))),
			wants: true,
		},
		{ // 4
			r1:    Resources(Resource(Name("disks"), ValueSet("sda1"), Role("*"))),
			r2:    Resources(Resource(Name("disks"), ValueSet("sda1"), Role("*"))),
			wants: true,
		},
		{ // 5
			r1:    Resources(Resource(Name("disks"), ValueSet("sda1"), Role("*"))),
			r2:    Resources(Resource(Name("disks"), ValueSet("sda2"), Role("*"))),
			wants: false,
		},
		{Resources(disks[0]), Resources(disks[1]), true},  // 6
		{Resources(disks[1]), Resources(disks[2]), true},  // 7
		{Resources(disks[4]), Resources(disks[5]), true},  // 8
		{Resources(disks[5]), Resources(disks[6]), false}, // 9
		{Resources(disks[3]), Resources(disks[6]), false}, // 10
		{ // 11
			r1:    Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable())),
			r2:    Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable())),
			wants: true,
		},
		{ // 12
			r1:    Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable())),
			r2:    Resources(Resource(Name("cpus"), ValueScalar(1), Role("*"))),
			wants: false,
		},
	} {
		actual := tc.r1.Equivalent(tc.r2)
		Expect(t, tc.wants == actual, "test case %d failed: wants (%v) != actual (%v)", i, tc.wants, actual)
	}

	possiblyReserved := mesos.Resources{
		// unreserved
		Resource(Name("cpus"), ValueScalar(8), Role("*")),
		// statically role reserved
		Resource(Name("cpus"), ValueScalar(8), Role("role1")),
		Resource(Name("cpus"), ValueScalar(8), Role("role2")),
		// dynamically role reserved:
		Resource(Name("cpus"), ValueScalar(8), Role("role1"), Reservation(ReservedBy("principal1"))),
		Resource(Name("cpus"), ValueScalar(8), Role("role2"), Reservation(ReservedBy("principal2"))),
	}
	for i := 0; i < len(possiblyReserved); i++ {
		for j := 0; j < len(possiblyReserved); j++ {
			if i == j {
				continue
			}
			if Resources(possiblyReserved[i]).Equivalent(Resources(possiblyReserved[j])) {
				t.Errorf("unexpected equivalence between %v and %v", possiblyReserved[i], possiblyReserved[j])
			}
		}
	}
}

func TestResources_ContainsAll(t *testing.T) {
	var (
		ports1 = Resources(Resource(Name("ports"), ValueRange(Span(2, 2), Span(4, 5)), Role("*")))
		ports2 = Resources(Resource(Name("ports"), ValueRange(Span(1, 10)), Role("*")))
		ports3 = Resources(Resource(Name("ports"), ValueRange(Span(2, 3)), Role("*")))
		ports4 = Resources(Resource(Name("ports"), ValueRange(Span(1, 2), Span(4, 6)), Role("*")))
		ports5 = Resources(Resource(Name("ports"), ValueRange(Span(1, 4), Span(5, 5)), Role("*")))

		disks1 = Resources(Resource(Name("disks"), ValueSet("sda1", "sda2"), Role("*")))
		disks2 = Resources(Resource(Name("disks"), ValueSet("sda1", "sda3", "sda4", "sda2"), Role("*")))

		disks = mesos.Resources{
			Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("1", "path")),
			Resource(Name("disk"), ValueScalar(10), Role("role"), Disk("2", "path")),
			Resource(Name("disk"), ValueScalar(20), Role("role"), Disk("1", "path")),
			Resource(Name("disk"), ValueScalar(20), Role("role"), Disk("", "path")),
			Resource(Name("disk"), ValueScalar(20), Role("role"), Disk("2", "path")),
		}
		summedDisks  = Resources(disks[0]).Plus(disks[1])
		summedDisks2 = Resources(disks[0]).Plus(disks[4])

		revocables = mesos.Resources{
			Resource(Name("cpus"), ValueScalar(1), Role("*"), Revocable()),
			Resource(Name("cpus"), ValueScalar(1), Role("*")),
			Resource(Name("cpus"), ValueScalar(2), Role("*")),
			Resource(Name("cpus"), ValueScalar(2), Role("*"), Revocable()),
		}
		summedRevocables  = Resources(revocables[0]).Plus(revocables[1])
		summedRevocables2 = Resources(revocables[0]).Plus(revocables[0])

		possiblyReserved = mesos.Resources{
			Resource(Name("cpus"), ValueScalar(8), Role("role")),
			Resource(Name("cpus"), ValueScalar(12), Role("role"), Reservation(ReservedBy("principal"))),
		}
		sumPossiblyReserved = Resources(possiblyReserved...)
	)
	for i, tc := range []struct {
		r1, r2 mesos.Resources
		wants  bool
	}{
		// test case 0
		{r1: nil, r2: nil, wants: true},
		// test case 1
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("*")),
				Resource(Name("mem"), ValueScalar(4096), Role("*")),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("*")),
				Resource(Name("mem"), ValueScalar(4096), Role("*")),
			),
			wants: true,
		},
		// test case 2
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("role1")),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("role2")),
			),
			wants: false,
		},
		// test case 3
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("*")),
				Resource(Name("mem"), ValueScalar(3072), Role("*")),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("*")),
				Resource(Name("mem"), ValueScalar(4096), Role("*")),
			),
			wants: false,
		},
		// test case 4
		{
			r1: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("*")),
				Resource(Name("mem"), ValueScalar(4096), Role("*")),
			),
			r2: Resources(
				Resource(Name("cpus"), ValueScalar(50), Role("*")),
				Resource(Name("mem"), ValueScalar(3072), Role("*")),
			),
			wants: true,
		},
		// test case 5
		{ports2, ports1, true},
		// test case 6
		{ports1, ports2, false},
		// test case 7
		{ports3, ports1, false},
		// test case 8
		{ports1, ports3, false},
		// test case 9
		{ports2, ports3, true},
		// test case 10
		{ports3, ports2, false},
		// test case 11
		{ports4, ports1, true},
		// test case 12
		{ports2, ports4, true},
		// test case 13
		{ports5, ports1, true},
		// test case 14
		{ports1, ports5, false},
		// test case 15
		{disks1, disks2, false},
		// test case 16
		{disks2, disks1, true},
		{r1: summedDisks, r2: Resources(disks[0]), wants: true},
		{r1: summedDisks, r2: Resources(disks[1]), wants: true},
		{r1: summedDisks, r2: Resources(disks[2]), wants: false},
		{r1: summedDisks, r2: Resources(disks[3]), wants: false},
		{r1: Resources(disks[0]), r2: summedDisks, wants: false},
		{r1: Resources(disks[1]), r2: summedDisks, wants: false},
		{r1: summedDisks2, r2: Resources(disks[0]), wants: true},
		{r1: summedDisks2, r2: Resources(disks[4]), wants: true},
		{r1: summedRevocables, r2: Resources(revocables[0]), wants: true},
		{r1: summedRevocables, r2: Resources(revocables[1]), wants: true},
		{r1: summedRevocables, r2: Resources(revocables[2]), wants: false},
		{r1: summedRevocables, r2: Resources(revocables[3]), wants: false},
		{r1: Resources(revocables[0]), r2: summedRevocables2, wants: false},
		{r1: summedRevocables2, r2: Resources(revocables[0]), wants: true},
		{r1: summedRevocables2, r2: summedRevocables2, wants: true},
		{r1: Resources(possiblyReserved[0]), r2: sumPossiblyReserved, wants: false},
		{r1: Resources(possiblyReserved[1]), r2: sumPossiblyReserved, wants: false},
		{r1: sumPossiblyReserved, r2: sumPossiblyReserved, wants: true},
	} {
		actual := tc.r1.ContainsAll(tc.r2)
		Expect(t, tc.wants == actual, "test case %d failed: wants (%v) != actual (%v)", i, tc.wants, actual)
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
		if !tc.wants.Equivalent(actual) {
			t.Errorf("test case %d failed: wants (%v) != actual (%v)", i, tc.wants, actual)
		}
		if !backup.Equivalent(tc.r1) {
			t.Errorf("test case %d failed: backup (%v) != r1 (%v)", i, backup, tc.r1)
		}

		// SubtractAll mutates the left operand
		tc.r1.Subtract(tc.r2...)
		if !tc.wants.Equivalent(tc.r1) {
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
		if !tc.wants.Equivalent(actual) {
			t.Errorf("test case %d failed: wants (%v) != actual (%v)", i, tc.wants, actual)
		}
		if !backup.Equivalent(tc.r1) {
			t.Errorf("test case %d failed: backup (%v) != r1 (%v)", i, backup, tc.r1)
		}

		// Add mutates the left operand
		tc.r1.Add(tc.r2...)
		if !tc.wants.Equivalent(tc.r1) {
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
