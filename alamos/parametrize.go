package alamos

// |||||| CONFIG ||||||

type ParametrizeConfig[V ParametrizeVars] interface {
	Next() (V, error)
}

// |||||| VARS ||||||

type ParametrizeVars interface{}

// |||||| PARAMETRIZE ||||||

func NewParametrize[V ParametrizeVars](config ParametrizeConfig[V]) *Parametrize[V] {
	return &Parametrize[V]{config: config}
}

type Parametrize[V ParametrizeVars] struct {
	config   ParametrizeConfig[V]
	template func(i int, vars V)
}

func (p *Parametrize[V]) Template(template func(i int, vars V)) {
	p.template = template
}

func (p *Parametrize[V]) Construct() {
	i := 0
	for {
		v, err := p.config.Next()
		if err != nil {
			break
		}
		p.template(i, v)
		i++
	}
}
