package skytable

import (
	"github.com/satvik007/skytable-go/internal/pool"
)

func (c *baseClient) Pool() pool.Pooler {
	return c.connPool
}
