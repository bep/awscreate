package awscreate

import "context"

// Provisioner is the interface that wraps the Create and Destroy methods.
type Provisioner[T any] interface {
	Create(ctx context.Context) (T, error)
	Destroy(ctx context.Context) error
}

type provisioner[T any] struct {
	create  func(ctx context.Context) (T, error)
	destroy func(ctx context.Context) error
}

func (p *provisioner[T]) Create(ctx context.Context) (T, error) {
	return p.create(ctx)
}

func (p *provisioner[T]) Destroy(ctx context.Context) error {
	return p.destroy(ctx)
}

// NewProvisioner returns a new Provisioner.
func NewProvisioner[T any](create func(ctx context.Context) (T, error), destroy func(ctx context.Context) error) Provisioner[T] {
	return &provisioner[T]{
		create:  create,
		destroy: destroy,
	}
}

type PolicyDocumentResource struct {
	Version   string
	Statement []PolicyDocumentResourceStatementEntry
}

type PolicyDocumentResourceStatementEntry struct {
	Effect    string
	Action    []string
	Principal map[string]any
	Resource  []string
}
