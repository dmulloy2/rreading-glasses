package internal

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/blampe/rreading-glasses/hardcover"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSearchHitIsCached(t *testing.T) {
	ctx := t.Context()

	// No expectations: any upstream query fails the test.
	gql := hardcover.NewMockgql(gomock.NewController(t))
	cache := newMemoryCache()
	getter, err := NewHardcoverGetter(cache, gql)
	require.NoError(t, err)

	work, err := json.Marshal(workResource{ForeignID: 42, BestBookID: 4200, Authors: []AuthorResource{{ForeignID: 7}}})
	require.NoError(t, err)
	cache.Set(ctx, WorkKey(42), work, time.Hour)

	want := SearchResource{BookID: 4200, WorkID: 42, Author: SearchResourceAuthor{ID: 7}}

	hit, ok := getter.searchHit(ctx, 42)
	require.True(t, ok)
	assert.Equal(t, want, hit)

	// With the work itself gone, the next search is answered from the hit it
	// left behind rather than from Hardcover.
	require.NoError(t, cache.Expire(ctx, WorkKey(42)))

	hit, ok = getter.searchHit(ctx, 42)
	require.True(t, ok)
	assert.Equal(t, want, hit)

	_, stillThere := cache.Get(ctx, WorkKey(42))
	assert.False(t, stillThere, "a search hit never stands in for the work")
}
