//go:build kitex

package plugin_transcode

import (
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	m7s "m7s.live/v5"
	transcode "m7s.live/v5/plugin/transcode/pkg"
)

var _ = m7s.InstallPlugin[TranscodePlugin](m7s.PluginMeta{
	NewTransformer:      transcode.NewTransform,
	RegisterGRPCHandler: RegisterService,
	ServiceDesc: &rpcinfo.EndpointBasicInfo{
		// TODO FENG 添加服务名
		ServiceName: "transcode.svc",
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

type TranscodePlugin struct {
	m7s.Plugin
	LogToFile string
}
