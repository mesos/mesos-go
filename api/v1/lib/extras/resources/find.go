package resources

import (
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/resourcefilters"
)

func Find(wants mesos.Resources, from ...mesos.Resource) (total mesos.Resources) {
	for i := range wants {
		found := find(wants[i], from...)

		// each want *must* be found
		if len(found) == 0 {
			return nil
		}

		total.Add(found...)
	}
	return total
}

func find(want mesos.Resource, from ...mesos.Resource) mesos.Resources {
	var (
		total      = mesos.Resources(from).Clone()
		remaining  = mesos.Resources{want}.Flatten()
		found      mesos.Resources
		predicates = resourcefilters.Filters{
			resourcefilters.ReservedByRole(want.GetRole()),
			resourcefilters.Unreserved,
			resourcefilters.Any,
		}
	)
	for _, predicate := range predicates {
		filtered := resourcefilters.Select(predicate, total...)
		for i := range filtered {
			// need to flatten to ignore the roles in ContainsAll()
			flattened := mesos.Resources{filtered[i]}.Flatten()
			if flattened.ContainsAll(remaining) {
				// want has been found, return the result
				return found.Add(remaining.Flatten(
					mesos.RoleName(filtered[i].GetRole()).Assign(),
					filtered[i].Reservation.Assign())...)
			}
			if remaining.ContainsAll(flattened) {
				found.Add1(filtered[i])
				total.Subtract1(filtered[i])
				remaining.Subtract(flattened...)
				break
			}
		}
	}
	return nil
}
