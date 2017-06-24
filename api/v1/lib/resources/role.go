package resources

type Role string

const RoleDefault = Role("*")

func (r Role) IsDefault() bool {
	return r == RoleDefault
}

func (r Role) Assign() func(interface{}) {
	return func(v interface{}) {
		type roler interface {
			WithRole(string)
		}
		if ri, ok := v.(roler); ok {
			ri.WithRole(string(r))
		}
	}
}

func (r Role) Proto() *string {
	s := string(r)
	return &s
}
