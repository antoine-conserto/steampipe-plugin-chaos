package chaos

import (
	"context"
	"errors"
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

type ItemBugCacheSum struct {
	Id     string
	Amount float64
	Page   int
}

type ListBugCacheSumResponse struct {
	Items []ItemBugCacheSum
	Resp  *PagingResponse
}

var maxPagesBugCacheSum = 4
var pageSize = 2500
var noMorePagesBugCacheSum = -1

var failureCountBugCacheSum = 5
var errorCountBugCacheSum = 0
var errorAfterPagesBugCacheSum = 3

func listBugCacheSumTable() *plugin.Table {
	return &plugin.Table{
		Name:             "chaos_bug_cache_sum",
		DefaultTransform: transform.FromValue(),
		Cache: &plugin.TableCacheOptions{
			Enabled: true,
		},
		List: &plugin.ListConfig{
			Hydrate: listBugCacheSumFunction,
			KeyColumns: []*plugin.KeyColumn{
				{Name: "id", Require: plugin.Optional, Operators: []string{"="}},
				{Name: "amount", Require: plugin.Optional, Operators: []string{"=", "!=", "<>", ">", ">=", "<", "<="}},
				{Name: "page", Require: plugin.Optional, Operators: []string{"=", "!=", "<>", ">", ">=", "<", "<="}},
			},
		},
		Columns: []*plugin.Column{
			{Name: "id", Type: proto.ColumnType_STRING, Hydrate: hydrateItemBugCacheSumId},
			{Name: "amount", Type: proto.ColumnType_DOUBLE, Hydrate: hydrateItemBugCacheSumAmount},
			{Name: "page", Type: proto.ColumnType_INT, Hydrate: hydrateItemBugCacheSumPage},
		},
	}
}

func listBugCacheSumFunction(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	nextPage := 0

	// define function to fetch a page of data - we will pass this to the sdk RetryHydrate call
	listPage := func(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
		items, resp, err := getPageBugCacheSum(nextPage)
		return ListBugCacheSumResponse{
			Items: items,
			Resp:  resp,
		}, err
	}

	for {
		retryResp, err := plugin.RetryHydrate(ctx, d, h, listPage, &plugin.RetryConfig{ShouldRetryError: shouldRetryErrorLegacy})
		listResponse := retryResp.(ListBugCacheSumResponse)
		items := listResponse.Items
		resp := listResponse.Resp

		if err != nil {
			return nil, err
		}

		for _, i := range items {
			d.StreamListItem(ctx, i)
		}
		if nextPage = resp.NextPage; nextPage == noMorePagesBugCacheSum {
			break
		}
	}
	return nil, nil
}

// This is a proxy of the API function to fetch the results
func getPageBugCacheSum(pageNumber int) ([]ItemBugCacheSum, *PagingResponse, error) {

	if pageNumber == maxPagesBugCacheSum {
		return nil, nil, errors.New("invalid page")
	}

	// after returning 3 pages, fail 5 times to return the next page before succeeding on the 6th attempt
	if pageNumber == errorAfterPagesBugCacheSum && errorCountBugCacheSum < failureCountBugCacheSum {
		errorCountBugCacheSum++
		return nil, nil, errors.New(retriableErrorString)
	}

	var items []ItemBugCacheSum
	for i := 0; i < pageSize; i++ {
		items = append(items, ItemBugCacheSum{Id: fmt.Sprintf("%d_%d", pageNumber, i), Amount: 10.00 * (float64(pageNumber) + 0.5), Page: pageNumber})
	}
	nextPage := pageNumber + 1
	if nextPage == maxPagesBugCacheSum {
		nextPage = noMorePagesBugCacheSum
	}
	response := PagingResponse{NextPage: nextPage}

	return items, &response, nil

}

func hydrateItemBugCacheSumId(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	return h.Item.(ItemBugCacheSum).Id, nil
}

func hydrateItemBugCacheSumAmount(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	return h.Item.(ItemBugCacheSum).Amount, nil
}

func hydrateItemBugCacheSumPage(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	return h.Item.(ItemBugCacheSum).Page, nil
}
