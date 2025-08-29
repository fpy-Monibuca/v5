//go:build kitex
// +build kitex

package plugin_flv

import (
	"fmt"
	"github.com/cloudwego/kitex/server"
	"m7s.live/v5/plugin/flv/pb"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	m7s "m7s.live/v5"
	"m7s.live/v5/pkg/util"
	. "m7s.live/v5/plugin/flv/pkg"

	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

type FLVPlugin struct {
	pb.UnimplementedApiServer

	m7s.Plugin
	Path string
}

const defaultConfig m7s.DefaultYaml = `publish:
  speed: 1`

var _ = m7s.InstallPlugin[FLVPlugin](m7s.PluginMeta{
	DefaultYaml:              defaultConfig,
	NewPuller:                NewPuller,
	NewRecorder:              NewRecorder,
	RegisterGRPCHandler:      pb.RegisterApiHandler,
	ServiceDesc:              &pb.Api_ServiceDesc,
	KitexRegisterGRPCHandler: RegisterService,
	KitexServiceDesc: &rpcinfo.EndpointBasicInfo{
		// TODO FENG 添加服务名
		ServiceName: "flv.svc",
	},
	NewPullProxy: m7s.NewHTTPPullPorxy,
})

func RegisterService(svr server.Server, plugin m7s.IPlugin, opts ...server.RegisterOption) error {
	//gb, ok := plugin.(*GB28181Plugin)
	//if !ok {
	//	return fmt.Errorf("plugin is not of type *GB28181Plugin")
	//}
	//return pb.RegisterService(svr, gb, opts...)
	return nil
}

func (plugin *FLVPlugin) OnInit() (err error) {
	_, port, _ := strings.Cut(plugin.GetCommonConf().HTTP.ListenAddr, ":")
	if port == "80" {
		plugin.PlayAddr = append(plugin.PlayAddr, "http://{hostName}/flv/{streamPath}", "ws://{hostName}/flv/{streamPath}")
	} else if port != "" {
		plugin.PlayAddr = append(plugin.PlayAddr, fmt.Sprintf("http://{hostName}:%s/flv/{streamPath}", port), fmt.Sprintf("ws://{hostName}:%s/flv/{streamPath}", port))
	}
	_, port, _ = strings.Cut(plugin.GetCommonConf().HTTP.ListenAddrTLS, ":")
	if port == "443" {
		plugin.PlayAddr = append(plugin.PlayAddr, "https://{hostName}/flv/{streamPath}", "wss://{hostName}/flv/{streamPath}")
	} else if port != "" {
		plugin.PlayAddr = append(plugin.PlayAddr, fmt.Sprintf("https://{hostName}:%s/flv/{streamPath}", port), fmt.Sprintf("wss://{hostName}:%s/flv/{streamPath}", port))
	}
	return
}

func (plugin *FLVPlugin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	streamPath := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), ".flv")
	var err error
	defer func() {
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}()
	var conn net.Conn
	var live Live
	if r.URL.RawQuery != "" {
		streamPath += "?" + r.URL.RawQuery
	}
	live.Subscriber, err = plugin.Subscribe(r.Context(), streamPath)
	if err != nil {
		return
	}
	live.Subscriber.RemoteAddr = r.RemoteAddr
	conn, err = live.Subscriber.CheckWebSocket(w, r)
	if err != nil {
		return
	}
	if conn != nil {
		live.WriteFlvTag = func(flv net.Buffers) (err error) {
			return wsutil.WriteServerMessage(conn, ws.OpBinary, util.ConcatBuffers(flv))
		}
		err = live.Run()
		return
	}
	wto := plugin.GetCommonConf().WriteTimeout
	if conn == nil {
		w.Header().Set("Content-Type", "video/x-flv")
		w.Header().Set("Transfer-Encoding", "identity")
		w.WriteHeader(http.StatusOK)
		if hijacker, ok := w.(http.Hijacker); ok && wto > 0 {
			conn, _, _ = hijacker.Hijack()
			conn.SetWriteDeadline(time.Now().Add(wto))
		}
	}
	if conn == nil {
		live.WriteFlvTag = func(flv net.Buffers) (err error) {
			_, err = flv.WriteTo(w)
			return
		}
		w.(http.Flusher).Flush()
	} else {
		live.WriteFlvTag = func(flv net.Buffers) (err error) {
			conn.SetWriteDeadline(time.Now().Add(wto))
			_, err = flv.WriteTo(conn)
			return
		}
	}
	err = live.Run()
}
