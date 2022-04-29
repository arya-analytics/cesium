package shut

type Group struct {
	children []Shutdown
}

func NewGroup(children ...Shutdown) *Group {
	return &Group{children: children}
}

func (g *Group) Sequential() error {
	for _, child := range g.children {
		if err := child.Shutdown(); err != nil {
			return err
		}
	}
	return nil
}
