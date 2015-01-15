package callback

type Password struct {
	echo     bool // show the password while it's entered
	password []byte
	prompt   string // prompt shown during password request
}

func NewPassword(prompt string, echo bool) *Password {
	return &Password{
		echo:   echo,
		prompt: prompt,
	}
}

func (cb *Password) Get() []byte {
	clone := make([]byte, len(cb.password))
	copy(clone, cb.password)
	return clone
}

func (cb *Password) Set(password []byte) {
	cb.password = make([]byte, len(password))
	copy(cb.password, password)
}

func (cb *Password) Prompt() string {
	return cb.prompt
}

func (cb *Password) EchoOn() bool {
	return cb.echo
}
