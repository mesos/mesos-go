package callback

type Name struct {
	defaultName string
	name        string
	prompt      string
}

func NewName(prompt, defaultName string) *Name {
	return &Name{
		defaultName: defaultName,
		prompt:      prompt,
	}
}

func (cb *Name) DefaultName() string {
	return cb.defaultName
}

func (cb *Name) Get() string {
	return cb.name
}

func (cb *Name) Set(name string) {
	cb.name = name
}

func (cb *Name) Prompt() string {
	return cb.prompt
}
