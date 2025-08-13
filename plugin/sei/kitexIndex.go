//go:build kitex

package plugin_sei

import (
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	"m7s.live/v5"
	sei "m7s.live/v5/plugin/sei/pkg"
)

var _ = m7s.InstallPlugin[SEIPlugin](m7s.PluginMeta{
	NewTransformer:      sei.NewTransform,
	RegisterGRPCHandler: RegisterService,
	ServiceDesc: &rpcinfo.EndpointBasicInfo{
		// TODO FENG 添加服务名
		ServiceName: "sei.svc",
	},
})

func RegisterService(svr server.Server, plugin m7s.IPlugin, opts ...server.RegisterOption) error {
	//gb, ok := plugin.(*GB28181Plugin)
	//if !ok {
	//	return fmt.Errorf("plugin is not of type *GB28181Plugin")
	//}
	//return pb.RegisterService(svr, gb, opts...)
	return nil
}

type SEIPlugin struct {
	m7s.Plugin
}
