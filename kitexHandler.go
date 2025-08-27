//go:build kitex
// +build kitex

package m7s

import (
	"context"
	"gitee.com/fpy-go/kitex_proto/kitex_gen/example/shop/item"
	"gitee.com/fpy-go/kitex_proto/kitex_gen/example/shop/stock"
	"gitee.com/fpy-go/kitex_proto/kitex_gen/example/shop/stock/stockservice"
	"github.com/cloudwego/kitex/client"
	etcd "github.com/kitex-contrib/registry-etcd"
	"log"
)

// ItemServiceImpl implements the last service interface defined in the IDL.
type ItemServiceImpl struct {
	stockCli stockservice.Client
}

// GetItem implements the ItemServiceImpl interface.
func (s *ItemServiceImpl) GetItem(ctx context.Context, req *item.GetItemReq) (resp *item.GetItemResp, err error) {
	resp = item.NewGetItemResp()
	resp.Item = item.NewItem()
	resp.Item.Id = req.GetId()
	resp.Item.Title = "Kitex"
	resp.Item.Description = "Kitex is an excellent framework!"

	stockReq := stock.NewGetItemStockReq()
	stockReq.ItemId = req.GetId()
	stockResp, err := s.stockCli.GetItemStock(context.Background(), stockReq)
	if err != nil {
		log.Println(err)
		stockResp.Stock = 0
	}
	resp.Item.Stock = stockResp.GetStock()
	return
}

// StockServiceImpl implements the last service interface defined in the IDL.
type StockServiceImpl struct{}

// GetItemStock implements the StockServiceImpl interface.
func (s *StockServiceImpl) GetItemStock(ctx context.Context, req *stock.GetItemStockReq) (resp *stock.GetItemStockResp, err error) {
	resp = stock.NewGetItemStockResp()
	resp.Stock = req.GetItemId()
	return
}

func NewStockClient(addr string) (stockservice.Client, error) {
	// 使用时请传入真实 etcd 的服务地址，本例中为 127.0.0.1:2379
	r, err := etcd.NewEtcdResolver([]string{"127.0.0.1:2379"})
	if err != nil {
		log.Fatal(err)
	}
	return stockservice.NewClient("gb28181.com", client.WithHostPorts(addr), client.WithResolver(r))
}
