//go:build kitex

package plugin_gb28181pro

import (
	"context"
	"fmt"
	pb "gitee.com/fpy-go/kitex_proto/kitex_gen/plugin/gb28181/pb"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"m7s.live/v5/pkg/config"
	"m7s.live/v5/pkg/util"
	gb28181 "m7s.live/v5/plugin/gb28181/pkg"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// TODO feng 总体需要优化的地方还很多，但是最先优化的就是参数校验问题，使用jupiter_proto生成validate文件

// List implements the GB28181Plugin interface.
func (s *GB28181Plugin) List(ctx context.Context, req *pb.GetDevicesRequest) (resp *pb.DevicesPageInfo, err error) {
	// TODO: Your code here...
	resp = &pb.DevicesPageInfo{}

	if s.DB == nil {
		resp.Code = 500
		resp.Message = "数据库未初始化"
		return resp, nil
	}

	var devices []Device
	var total int64

	// 构建查询条件
	query := s.DB.Model(&Device{})
	if req.Query != "" {
		query = query.Where("device_id LIKE ? OR name LIKE ?",
			"%"+req.Query+"%", "%"+req.Query+"%")
	}
	if req.Status {
		query = query.Where("online = ?", true)
	}

	// 获取总数
	if err := query.Count(&total).Error; err != nil {
		resp.Code = 500
		resp.Message = fmt.Sprintf("查询总数失败: %v", err)
		return resp, nil
	}

	// 查询设备列表
	// 当Page和Count都为0时，不做分页，返回所有数据
	if req.Page == 0 && req.Count == 0 {
		// 不分页，查询所有数据
		if err := query.Find(&devices).Error; err != nil {
			resp.Code = 500
			resp.Message = fmt.Sprintf("查询设备列表失败: %v", err)
			return resp, nil
		}
	} else {
		// 分页查询设备列表
		if err := query.
			Offset(int(req.Page-1) * int(req.Count)).
			Limit(int(req.Count)).
			Find(&devices).Error; err != nil {
			resp.Code = 500
			resp.Message = fmt.Sprintf("查询设备列表失败: %v", err)
			return resp, nil
		}
	}

	// 转换为proto消息
	var pbDevices []*pb.Device
	for _, d := range devices {
		// 查询设备对应的通道
		var channels []gb28181.DeviceChannel
		if err := s.DB.Where(&gb28181.DeviceChannel{DeviceID: d.DeviceId}).Find(&channels).Error; err != nil {
			s.Error("查询通道失败", "error", err)
			continue
		}

		var pbChannels []*pb.Channel
		for _, c := range channels {
			pbChannels = append(pbChannels, &pb.Channel{
				DeviceId:     c.ChannelID,
				ParentId:     c.DeviceID,
				ChannelId:    c.ChannelID,
				Name:         c.Name,
				Manufacturer: c.Manufacturer,
				Model:        c.Model,
				Owner:        c.Owner,
				CivilCode:    c.CivilCode,
				Address:      c.Address,
				Port:         int32(c.Port),
				Parental:     int32(c.Parental),
				SafetyWay:    int32(c.SafetyWay),
				RegisterWay:  int32(c.RegisterWay),
				Secrecy:      int32(c.Secrecy),
				Status:       string(c.Status),
				Longitude:    fmt.Sprintf("%f", c.GbLongitude),
				Latitude:     fmt.Sprintf("%f", c.GbLatitude),
				GpsTime:      timestamppb.New(time.Now()),
			})
		}

		pbDevices = append(pbDevices, &pb.Device{
			DeviceId:      d.DeviceId,
			Name:          d.Name,
			Manufacturer:  d.Manufacturer,
			Model:         d.Model,
			Status:        string(d.Status),
			Online:        d.Online,
			Longitude:     d.Longitude,
			Latitude:      d.Latitude,
			RegisterTime:  timestamppb.New(d.RegisterTime),
			UpdateTime:    timestamppb.New(d.UpdateTime),
			KeepAliveTime: timestamppb.New(d.KeepaliveTime),
			ChannelCount:  int32(d.ChannelCount),
			Channels:      pbChannels,
			MediaIp:       d.MediaIp,
			SipIp:         d.SipIp,
			Password:      d.Password,
			StreamMode:    d.StreamMode,
		})
	}

	resp.Code = 0
	resp.Message = "success"
	resp.Total = int32(total)
	resp.Data = pbDevices

	return resp, nil
}

// PsReplay implements the ApiImpl interface.
func (s *GB28181Plugin) PsReplay(ctx context.Context, req *pb.PsReplayRequest) (resp *pb.PsReplayResponse, err error) {
	dump := req.GetDump()
	streamPath := req.GetStreamPath()
	if dump == "" {
		dump = "dump/ps"
	}
	if streamPath == "" {
		if strings.HasPrefix(dump, "/") {
			streamPath = "replay" + dump
		} else {
			streamPath = "replay/" + dump
		}
	}
	var puller gb28181.DumpPuller
	puller.GetPullJob().Init(&puller, &s.Plugin, streamPath, config.Pull{
		URL: dump,
	}, nil)
	return nil, nil
}

// GetDevice implements the GB28181Plugin interface.
func (gb *GB28181Plugin) GetDevice(ctx context.Context, req *pb.GetDeviceRequest) (resp *pb.DeviceResponse, err error) {
	resp = &pb.DeviceResponse{}
	// 先从内存中获取
	d, ok := gb.devices.Get(req.DeviceId)
	if !ok && gb.DB != nil {
		// 如果内存中没有且数据库存在，则从数据库查询
		var device Device
		if err := gb.DB.Where("device_id = ?", req.DeviceId).First(&device).Error; err == nil {
			d = &device
		}
	}

	if d != nil {
		var channels []*pb.Channel
		for c := range d.channels.Range {
			channels = append(channels, &pb.Channel{
				DeviceId:     c.DeviceID,
				ParentId:     c.ParentID,
				Name:         c.Name,
				Manufacturer: c.Manufacturer,
				Model:        c.Model,
				Owner:        c.Owner,
				CivilCode:    c.CivilCode,
				Address:      c.Address,
				Port:         int32(c.Port),
				Parental:     int32(c.Parental),
				SafetyWay:    int32(c.SafetyWay),
				RegisterWay:  int32(c.RegisterWay),
				Secrecy:      int32(c.Secrecy),
				Status:       string(c.Status),
				Longitude:    fmt.Sprintf("%f", c.GbLongitude),
				Latitude:     fmt.Sprintf("%f", c.GbLatitude),
				GpsTime:      timestamppb.New(time.Now()),
			})
		}
		resp.Data = &pb.Device{
			DeviceId:     d.DeviceId,
			Name:         d.Name,
			Manufacturer: d.Manufacturer,
			Model:        d.Model,
			Status:       string(d.Status),
			Online:       d.Online,
			Longitude:    d.Longitude,
			Latitude:     d.Latitude,
			RegisterTime: timestamppb.New(d.RegisterTime),
			UpdateTime:   timestamppb.New(d.UpdateTime),
			Channels:     channels,
			MediaIp:      d.MediaIp,
			SipIp:        d.SipIp,
			Password:     d.Password,
			StreamMode:   d.StreamMode,
		}
		resp.Code = 0
		resp.Message = "success"
	} else {
		resp.Code = 404
		resp.Message = "device not found"
	}
	return resp, nil
}

// GetDevices implements the GB28181Plugin interface.
func (gb *GB28181Plugin) GetDevices(ctx context.Context, req *pb.GetDevicesRequest) (resp *pb.DevicesPageInfo, err error) {
	resp = &pb.DevicesPageInfo{}

	if gb.DB == nil {
		resp.Code = 500
		resp.Message = "数据库未初始化"
		return resp, nil
	}

	var devices []Device
	var total int64

	// 构建查询条件
	query := gb.DB.Model(&Device{})
	if req.Query != "" {
		query = query.Where("device_id LIKE ? OR name LIKE ?",
			"%"+req.Query+"%", "%"+req.Query+"%")
	}
	if req.Status {
		query = query.Where("online = ?", true)
	}

	// 获取总数
	if err := query.Count(&total).Error; err != nil {
		resp.Code = 500
		resp.Message = fmt.Sprintf("查询总数失败: %v", err)
		return resp, nil
	}

	// 查询设备列表
	// 当Page和Count都为0时，不做分页，返回所有数据
	if req.Page == 0 && req.Count == 0 {
		// 不分页，查询所有数据
		if err := query.Find(&devices).Error; err != nil {
			resp.Code = 500
			resp.Message = fmt.Sprintf("查询设备列表失败: %v", err)
			return resp, nil
		}
	} else {
		// 分页查询设备，并预加载通道数据
		if err := query.
			Offset(int(req.Page-1) * int(req.Count)).
			Limit(int(req.Count)).
			Find(&devices).Error; err != nil {
			resp.Code = 500
			resp.Message = fmt.Sprintf("查询设备列表失败: %v", err)
			return resp, nil
		}
	}

	// 转换为proto消息
	var pbDevices []*pb.Device
	for _, d := range devices {
		// 查询设备对应的通道
		var channels []gb28181.DeviceChannel
		if err := gb.DB.Where(&gb28181.DeviceChannel{DeviceID: d.DeviceId}).Find(&channels).Error; err != nil {
			gb.Error("查询通道失败", "error", err)
			continue
		}

		var pbChannels []*pb.Channel
		for _, c := range channels {
			pbChannels = append(pbChannels, &pb.Channel{
				DeviceId:     c.ChannelID,
				ParentId:     c.ParentID,
				Name:         c.Name,
				Manufacturer: c.Manufacturer,
				Model:        c.Model,
				Owner:        c.Owner,
				CivilCode:    c.CivilCode,
				Address:      c.Address,
				Port:         int32(c.Port),
				Parental:     int32(c.Parental),
				SafetyWay:    int32(c.SafetyWay),
				RegisterWay:  int32(c.RegisterWay),
				Secrecy:      int32(c.Secrecy),
				Status:       string(c.Status),
				Longitude:    fmt.Sprintf("%f", c.GbLongitude),
				Latitude:     fmt.Sprintf("%f", c.GbLatitude),
				GpsTime:      timestamppb.New(time.Now()),
			})
		}

		pbDevice := &pb.Device{
			DeviceId:      d.DeviceId,
			Name:          d.Name,
			Manufacturer:  d.Manufacturer,
			Model:         d.Model,
			Status:        string(d.Status),
			Online:        d.Online,
			Longitude:     d.Longitude,
			Latitude:      d.Latitude,
			RegisterTime:  timestamppb.New(d.RegisterTime),
			UpdateTime:    timestamppb.New(d.UpdateTime),
			KeepAliveTime: timestamppb.New(d.KeepaliveTime),
			Channels:      pbChannels,
			MediaIp:       d.MediaIp,
			SipIp:         d.SipIp,
			Password:      d.Password,
			StreamMode:    d.StreamMode,
		}
		pbDevices = append(pbDevices, pbDevice)
	}

	resp.Total = int32(total)
	resp.Data = pbDevices
	resp.Code = 0
	resp.Message = "success"
	return resp, nil
}

// GetChannels 实现分页查询通道
func (gb *GB28181Plugin) GetChannels(ctx context.Context, req *pb.GetChannelsRequest) (resp *pb.ChannelsPageInfo, err error) {
	resp = &pb.ChannelsPageInfo{}

	// 先从内存中获取
	d, ok := gb.devices.Get(req.DeviceId)
	if !ok && gb.DB != nil {
		// 如果内存中没有且数据库存在，则从数据库查询
		var device Device
		if err := gb.DB.Where(Device{DeviceId: req.DeviceId}).First(&device).Error; err == nil {
			d = &device
		}
	}

	if d != nil {
		// 查询设备对应的通道
		var cs []gb28181.DeviceChannel
		// 构建查询条件
		query := gb.DB.Model(&gb28181.DeviceChannel{})
		query = query.Where("device_id = ?", d.DeviceId)
		if req.Query != "" {
			query = query.Where("name LIKE ? OR device_id LIKE ?",
				"%"+req.Query+"%", "%"+req.Query+"%")
		}

		if req.Online {
			query = query.Where("status = 'ON'")
		}

		if req.ChannelType {
			query = query.Where("parent_id != ?", "").Where("parent_id IS NOT NULL")
		}

		var total int64
		// 获取总数
		if err := query.Count(&total).Error; err != nil {
			resp.Code = 500
			resp.Message = fmt.Sprintf("failed to count channels: %v", err)
			return resp, nil
		}

		// 查询平台列表
		// 当Page和Count都为0时，不做分页，返回所有数据
		if req.Page == 0 && req.Count == 0 {
			// 不分页，查询所有数据
			if err := query.Find(&cs).Error; err != nil {
				resp.Code = 500
				resp.Message = fmt.Sprintf("failed to list channel: %v", err)
				return resp, nil
			}
		} else {
			// 分页查询
			if err := query.Offset(int(req.Page-1) * int(req.Count)).
				Limit(int(req.Count)).
				Find(&cs).Error; err != nil {
				resp.Code = 500
				resp.Message = fmt.Sprintf("failed to list channel: %v", err)
				return resp, nil
			}
		}

		var channels []*pb.Channel
		for _, c := range cs {
			channels = append(channels, &pb.Channel{
				DeviceId:     c.DeviceID,
				ParentId:     c.ParentID,
				Name:         c.Name,
				Manufacturer: c.Manufacturer,
				Model:        c.Model,
				Owner:        c.Owner,
				CivilCode:    c.CivilCode,
				Address:      c.Address,
				Port:         int32(c.Port),
				Parental:     int32(c.Parental),
				SafetyWay:    int32(c.SafetyWay),
				RegisterWay:  int32(c.RegisterWay),
				Secrecy:      int32(c.Secrecy),
				Status:       string(c.Status),
				Longitude:    fmt.Sprintf("%f", c.GbLongitude),
				Latitude:     fmt.Sprintf("%f", c.GbLatitude),
				GpsTime:      timestamppb.New(time.Now()),
			})
		}

		resp.Total = int32(len(channels))
		resp.List = channels
		resp.Code = 0
		resp.Message = "success"
	} else {
		resp.Code = 404
		resp.Message = "device not found"
	}
	return resp, nil
}

// SyncDevice 实现同步设备通道信息
func (gb *GB28181Plugin) SyncDevice(ctx context.Context, req *pb.SyncDeviceRequest) (resp *pb.SyncStatus, err error) {
	resp = &pb.SyncStatus{
		Code:    404,
		Message: "device not found",
	}

	// 先从内存中获取设备
	d, ok := gb.devices.Get(req.DeviceId)
	if !ok && gb.DB != nil {
		// 如果内存中没有且数据库存在，则从数据库查询
		var device Device
		if err := gb.DB.Where("device_id = ?", req.DeviceId).First(&device).Error; err == nil {
			d = &device
			// 恢复设备的必要字段
			d.Logger = gb.Logger.With("deviceid", req.DeviceId)
			d.channels.L = new(sync.RWMutex)
			d.plugin = gb

			// 初始化 Task
			var hash uint32
			for i := 0; i < len(d.DeviceId); i++ {
				ch := d.DeviceId[i]
				hash = hash*31 + uint32(ch)
			}
			d.Task.ID = hash
			d.Task.Logger = d.Logger
			d.Task.Context, d.Task.CancelCauseFunc = context.WithCancelCause(context.Background())

			// 初始化 SIP 相关字段
			d.fromHDR = sip.FromHeader{
				Address: sip.Uri{
					User: gb.Serial,
					Host: gb.Realm,
				},
				Params: sip.NewParams(),
			}
			d.fromHDR.Params.Add("tag", sip.GenerateTagN(16))

			d.contactHDR = sip.ContactHeader{
				Address: sip.Uri{
					User: gb.Serial,
					Host: d.SipIp,
					Port: d.Port,
				},
			}

			d.Recipient = sip.Uri{
				Host: d.IP,
				Port: d.Port,
				User: d.DeviceId,
			}

			// 初始化 SIP 客户端
			d.client, _ = sipgo.NewClient(gb.ua, sipgo.WithClientLogger(zerolog.New(os.Stdout)), sipgo.WithClientHostname(d.SipIp))

			// 将设备添加到内存中
			gb.devices.Add(d)
		}
	}

	if d != nil {
		// 发送目录查询请求
		_, err := d.catalog()
		if err != nil {
			resp.Code = 500
			resp.Message = "catalog request failed"
			resp.ErrorMsg = err.Error()
		} else {
			resp.Code = 0
			resp.Message = "sync request sent"
			resp.Total = int32(d.ChannelCount)
			resp.Current = 0 // 初始化进度为0
		}
	}

	return resp, nil
}

// DeleteDevice implements the GB28181Plugin interface.
func (s *GB28181Plugin) DeleteDevice(ctx context.Context, req *pb.DeleteDeviceRequest) (resp *pb.DeleteDeviceResponse, err error) {
	// TODO: Your code here...
	return
}

// GetSubChannels implements the GB28181Plugin interface.
func (s *GB28181Plugin) GetSubChannels(ctx context.Context, req *pb.GetSubChannelsRequest) (resp *pb.ChannelsPageInfo, err error) {
	// TODO: Your code here...
	return
}

// ChangeAudio implements the GB28181Plugin interface.
func (s *GB28181Plugin) ChangeAudio(ctx context.Context, req *pb.ChangeAudioRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// UpdateChannelStreamIdentification implements the GB28181Plugin interface.
func (s *GB28181Plugin) UpdateChannelStreamIdentification(ctx context.Context, req *pb.Channel) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// UpdateTransport implements the GB28181Plugin interface.
func (s *GB28181Plugin) UpdateTransport(ctx context.Context, req *pb.UpdateTransportRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// AddDevice implements the GB28181Plugin interface.
func (s *GB28181Plugin) AddDevice(ctx context.Context, req *pb.Device) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// UpdateDevice 实现更新设备信息
func (gb *GB28181Plugin) UpdateDevice(ctx context.Context, req *pb.Device) (resp *pb.BaseResponse, err error) {
	resp = &pb.BaseResponse{}

	// 检查数据库连接
	if gb.DB == nil {
		resp.Code = 500
		resp.Message = "数据库未初始化"
		return resp, nil
	}

	// 先从缓存中读取设备
	if d, ok := gb.devices.Get(req.DeviceId); ok {
		// 保存原始密码，用于后续检查是否修改了密码
		originalPassword := d.Password

		// 更新基本字段
		if req.Name != "" {
			d.Name = req.Name
		}
		if req.Manufacturer != "" {
			d.Manufacturer = req.Manufacturer
		}
		if req.Model != "" {
			d.Model = req.Model
		}
		if req.Longitude != "" {
			d.Longitude = req.Longitude
		}
		if req.Latitude != "" {
			d.Latitude = req.Latitude
		}

		// 更新新增字段
		if req.MediaIp != "" {
			d.MediaIp = req.MediaIp
		}
		if req.SipIp != "" {
			d.SipIp = req.SipIp

			// 更新SIP相关字段
			d.contactHDR = sip.ContactHeader{
				Address: sip.Uri{
					User: gb.Serial,
					Host: d.SipIp,
					Port: d.Port,
				},
			}
		}
		if req.StreamMode != "" {
			d.StreamMode = req.StreamMode
		}
		if req.Password != "" {
			d.Password = req.Password
		}

		// 更新订阅相关字段
		if req.SubscribeCatalog {
			d.SubscribeCatalog = 3600 // 默认订阅周期为60分钟
		} else {
			d.SubscribeCatalog = 0 // 不订阅
		}

		if req.SubscribePosition {
			d.SubscribePosition = 3600 // 默认订阅周期为60分钟
		} else {
			d.SubscribePosition = 0 // 不订阅
		}

		//更新订阅报警信息的字段
		if req.SubscribeAlarm {
			d.SubscribeAlarm = 3600 // 默认订阅周期为60分钟
		} else {
			d.SubscribeAlarm = 0 // 不订阅
		}
		d.UpdateTime = time.Now()

		// 先停止设备任务
		//d.Stop(fmt.Errorf("device updated"))
		// 更新数据库中的设备信息
		updates := map[string]interface{}{
			"name":               d.Name,
			"manufacturer":       d.Manufacturer,
			"model":              d.Model,
			"longitude":          d.Longitude,
			"latitude":           d.Latitude,
			"media_ip":           d.MediaIp,
			"sip_ip":             d.SipIp,
			"stream_mode":        d.StreamMode,
			"password":           d.Password,
			"subscribe_catalog":  d.SubscribeCatalog,
			"subscribe_position": d.SubscribePosition,
			"subscribe_alarm":    d.SubscribeAlarm,
			"update_time":        d.UpdateTime,
		}

		if err := gb.DB.Model(&Device{}).Where("device_id = ?", req.DeviceId).Updates(updates).Error; err != nil {
			resp.Code = 500
			resp.Message = fmt.Sprintf("更新设备失败: %v", err)
			return resp, nil
		}

		// 检查密码是否被修改
		passwordChanged := req.Password != "" && req.Password != originalPassword

		// 如果密码没有被修改，则需要重新启动设备任务和订阅任务
		if !passwordChanged {
			// 重新启动设备任务
			//gb.AddTask(d)

			// 如果需要订阅目录，创建并启动目录订阅任务
			if d.Online {
				if d.SubscribeCatalog > 0 {
					if d.CatalogSubscribeTask != nil {
						d.CatalogSubscribeTask.Ticker.Reset(time.Second * time.Duration(d.SubscribeCatalog))
						d.CatalogSubscribeTask.Tick(nil)
					} else {
						catalogSubTask := NewCatalogSubscribeTask(d)
						d.AddTask(catalogSubTask)
						d.CatalogSubscribeTask.Tick(nil)
					}
				} else {
					if d.CatalogSubscribeTask != nil {
						d.CatalogSubscribeTask.Stop(fmt.Errorf("catalog subscription disabled"))
					}
				}
				if d.SubscribePosition > 0 {
					if d.PositionSubscribeTask != nil {
						d.PositionSubscribeTask.Ticker.Reset(time.Second * time.Duration(d.SubscribePosition))
						d.PositionSubscribeTask.Tick(nil)
					} else {
						positionSubTask := NewPositionSubscribeTask(d)
						d.AddTask(positionSubTask)
						d.PositionSubscribeTask.Tick(nil)
					}
				} else {
					if d.PositionSubscribeTask != nil {
						d.PositionSubscribeTask.Stop(fmt.Errorf("position subscription disabled"))
					}
				}
				if d.SubscribeAlarm > 0 {
					if d.AlarmSubscribeTask != nil {
						d.AlarmSubscribeTask.Ticker.Reset(time.Second * time.Duration(d.SubscribeAlarm))
						d.AlarmSubscribeTask.Tick(nil)
					} else {
						alarmSubTask := NewAlarmSubscribeTask(d)
						d.AddTask(alarmSubTask)
						d.AlarmSubscribeTask.Tick(nil)
					}
				} else {
					if d.AlarmSubscribeTask != nil {
						d.AlarmSubscribeTask.Stop(fmt.Errorf("alarm subscription disabled"))
					}
				}
			}
		} else {
			d.Stop(fmt.Errorf("password changed"))
		}

		resp.Code = 0
		resp.Message = "设备更新成功"
		return resp, nil
	}

	// 如果缓存中没有，则从数据库中查找设备
	var device Device
	if err := gb.DB.Where("device_id = ?", req.DeviceId).First(&device).Error; err != nil {
		// 如果数据库中也没有找到设备，返回错误
		resp.Code = 404
		resp.Message = fmt.Sprintf("设备不存在: %v", err)
		return resp, nil
	}

	// 如果数据库中找到了设备，直接更新数据库
	updates := map[string]interface{}{}

	// 更新基本字段
	if req.Name != "" {
		updates["name"] = req.Name
	}
	if req.Manufacturer != "" {
		updates["manufacturer"] = req.Manufacturer
	}
	if req.Model != "" {
		updates["model"] = req.Model
	}
	if req.Longitude != "" {
		updates["longitude"] = req.Longitude
	}

	if req.Latitude != "" {
		updates["latitude"] = req.Latitude
	}

	// 更新新增字段
	if req.MediaIp != "" {
		updates["media_ip"] = req.MediaIp
	}
	if req.SipIp != "" {
		updates["sip_ip"] = req.SipIp
	}
	if req.StreamMode != "" {
		updates["stream_mode"] = req.StreamMode
	}
	if req.Password != "" {
		updates["password"] = req.Password
	}

	// 更新订阅相关字段
	if req.SubscribeCatalog {
		updates["subscribe_catalog"] = 3600 // 默认订阅周期为3600秒
	} else {
		updates["subscribe_catalog"] = 0 // 不订阅
	}

	if req.SubscribePosition {
		updates["subscribe_position"] = 3600 // 默认订阅周期为3600秒
	} else {
		updates["subscribe_position"] = 0 // 不订阅
	}

	updates["update_time"] = time.Now()

	// 保存到数据库
	if err := gb.DB.Model(&device).Updates(updates).Error; err != nil {
		resp.Code = 500
		resp.Message = fmt.Sprintf("更新设备失败: %v", err)
		return resp, nil
	}

	resp.Code = 0
	resp.Message = "设备更新成功"
	return resp, nil
}

// GetDeviceStatus implements the GB28181Plugin interface.
func (s *GB28181Plugin) GetDeviceStatus(ctx context.Context, req *pb.GetDeviceStatusRequest) (resp *pb.DeviceStatusResponse, err error) {
	// TODO: Your code here...
	return
}

// GetDeviceAlarm implements the GB28181Plugin interface.
func (s *GB28181Plugin) GetDeviceAlarm(ctx context.Context, req *pb.GetDeviceAlarmRequest) (resp *pb.DeviceAlarmResponse, err error) {
	// TODO: Your code here...
	return
}

// GetSyncStatus implements the GB28181Plugin interface.
func (s *GB28181Plugin) GetSyncStatus(ctx context.Context, req *pb.GetSyncStatusRequest) (resp *pb.SyncStatus, err error) {
	// TODO: Your code here...
	return
}

// GetSubscribeInfo implements the GB28181Plugin interface.
func (s *GB28181Plugin) GetSubscribeInfo(ctx context.Context, req *pb.GetSubscribeInfoRequest) (resp *pb.SubscribeInfoResponse, err error) {
	// TODO: Your code here...
	return
}

// GetSnap implements the GB28181Plugin interface.
func (s *GB28181Plugin) GetSnap(ctx context.Context, req *pb.GetSnapRequest) (resp *pb.SnapResponse, err error) {
	// TODO: Your code here...
	return
}

// StopConvert implements the GB28181Plugin interface.
func (s *GB28181Plugin) StopConvert(ctx context.Context, req *pb.ConvertStopRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// StartBroadcast implements the GB28181Plugin interface.
func (s *GB28181Plugin) StartBroadcast(ctx context.Context, req *pb.BroadcastRequest) (resp *pb.BroadcastResponse, err error) {
	// TODO: Your code here...
	return
}

// StopBroadcast implements the GB28181Plugin interface.
func (s *GB28181Plugin) StopBroadcast(ctx context.Context, req *pb.BroadcastRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// GetAllSSRC implements the GB28181Plugin interface.
func (s *GB28181Plugin) GetAllSSRC(ctx context.Context, req *emptypb.Empty) (resp *pb.SSRCListResponse, err error) {
	// TODO: Your code here...
	return
}

// GetRawChannel implements the GB28181Plugin interface.
func (s *GB28181Plugin) GetRawChannel(ctx context.Context, req *pb.GetRawChannelRequest) (resp *pb.Channel, err error) {
	// TODO: Your code here...
	return
}

// AddPlatform 实现添加平台信息
func (gb *GB28181Plugin) AddPlatform(ctx context.Context, req *pb.Platform) (resp *pb.BaseResponse, err error) {
	resp = &pb.BaseResponse{}

	if gb.DB == nil {
		resp.Code = 500
		resp.Message = "database not initialized"
		return resp, nil
	}

	// 必填字段校验
	if req.Name == "" {
		resp.Code = 400
		resp.Message = "平台名称不可为空"
		return resp, nil
	}
	if req.ServerGBId == "" {
		resp.Code = 400
		resp.Message = "上级平台国标编号不可为空"
		return resp, nil
	}
	if req.ServerIp == "" {
		resp.Code = 400
		resp.Message = "上级平台IP不可为空"
		return resp, nil
	}
	if req.ServerPort <= 0 || req.ServerPort > 65535 {
		resp.Code = 400
		resp.Message = "上级平台端口异常"
		return resp, nil
	}
	if req.DeviceGBId == "" {
		resp.Code = 400
		resp.Message = "本平台国标编号不可为空"
		return resp, nil
	}

	// 检查平台是否已存在
	var existingPlatform gb28181.PlatformModel
	if err := gb.DB.Where("server_gb_id = ?", req.ServerGBId).First(&existingPlatform).Error; err == nil {
		resp.Code = 400
		resp.Message = fmt.Sprintf("平台 %s 已存在", req.ServerGBId)
		return resp, nil
	}

	// 设置默认值
	if req.ServerGBDomain == "" {
		req.ServerGBDomain = req.ServerGBId[:6] // 取前6位作为域
	}
	if req.Expires <= 0 {
		req.Expires = 3600 // 默认3600秒
	}
	if req.KeepTimeout <= 0 {
		req.KeepTimeout = 60 // 默认60秒
	}
	if req.Transport == "" {
		req.Transport = "UDP" // 默认UDP
	}
	if req.CharacterSet == "" {
		req.CharacterSet = "GB2312" // 默认GB2312
	}

	// 设置创建时间和更新时间
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	req.CreateTime = currentTime
	req.UpdateTime = currentTime

	// 将proto消息转换为数据库模型
	platformModel := &gb28181.PlatformModel{
		Enable:                  req.Enable,
		Name:                    req.Name,
		ServerGBID:              req.ServerGBId,
		ServerGBDomain:          req.ServerGBDomain,
		ServerIP:                req.ServerIp,
		ServerPort:              int(req.ServerPort),
		DeviceGBID:              req.DeviceGBId,
		DeviceIP:                req.DeviceIp,
		DevicePort:              int(req.DevicePort),
		Username:                req.Username,
		Password:                req.Password,
		Expires:                 int(req.Expires),
		KeepTimeout:             int(req.KeepTimeout),
		Transport:               req.Transport,
		CharacterSet:            req.CharacterSet,
		PTZ:                     req.Ptz,
		RTCP:                    req.Rtcp,
		Status:                  req.Status,
		ChannelCount:            int(req.ChannelCount),
		CatalogSubscribe:        req.CatalogSubscribe,
		AlarmSubscribe:          req.AlarmSubscribe,
		MobilePositionSubscribe: req.MobilePositionSubscribe,
		CatalogGroup:            int(req.CatalogGroup),
		UpdateTime:              req.UpdateTime,
		CreateTime:              req.CreateTime,
		AsMessageChannel:        req.AsMessageChannel,
		SendStreamIP:            req.SendStreamIp,
		AutoPushChannel:         req.AutoPushChannel,
		CatalogWithPlatform:     int(req.CatalogWithPlatform),
		CatalogWithGroup:        int(req.CatalogWithGroup),
		CatalogWithRegion:       int(req.CatalogWithRegion),
		CivilCode:               req.CivilCode,
		Manufacturer:            req.Manufacturer,
		Model:                   req.Model,
		Address:                 req.Address,
		RegisterWay:             int(req.RegisterWay),
		Secrecy:                 int(req.Secrecy),
	}

	// 保存到数据库
	if err := gb.DB.Create(platformModel).Error; err != nil {
		resp.Code = 500
		resp.Message = fmt.Sprintf("failed to create platform: %v", err)
		return resp, nil
	}

	// 如果平台启用，则创建Platform实例并启动任务
	if platformModel.Enable {
		// 创建Platform实例
		platform := NewPlatform(platformModel, gb, false)
		// 添加到任务系统
		gb.AddTask(platform)
	}

	resp.Code = 0
	resp.Message = "success"
	return resp, nil
}

// GetPlatform 实现获取平台信息
func (gb *GB28181Plugin) GetPlatform(ctx context.Context, req *pb.GetPlatformRequest) (resp *pb.PlatformResponse, err error) {
	resp = &pb.PlatformResponse{}

	if gb.DB == nil {
		resp.Code = 500
		resp.Message = "database not initialized"
		return resp, nil
	}

	var platform gb28181.PlatformModel
	if err := gb.DB.First(&platform, req.ServerGBID).Error; err != nil {
		resp.Code = 404
		resp.Message = "platform not found"
		return resp, nil
	}

	// 将数据库模型转换为proto消息
	resp.Data = &pb.Platform{
		Enable:                  platform.Enable,
		Name:                    platform.Name,
		ServerGBId:              platform.ServerGBID,
		ServerGBDomain:          platform.ServerGBDomain,
		ServerIp:                platform.ServerIP,
		ServerPort:              int32(platform.ServerPort),
		DeviceGBId:              platform.DeviceGBID,
		DeviceIp:                platform.DeviceIP,
		DevicePort:              int32(platform.DevicePort),
		Username:                platform.Username,
		Password:                platform.Password,
		Expires:                 int32(platform.Expires),
		KeepTimeout:             int32(platform.KeepTimeout),
		Transport:               platform.Transport,
		CharacterSet:            platform.CharacterSet,
		Ptz:                     platform.PTZ,
		Rtcp:                    platform.RTCP,
		Status:                  platform.Status,
		ChannelCount:            int32(platform.ChannelCount),
		CatalogSubscribe:        platform.CatalogSubscribe,
		AlarmSubscribe:          platform.AlarmSubscribe,
		MobilePositionSubscribe: platform.MobilePositionSubscribe,
		CatalogGroup:            int32(platform.CatalogGroup),
		UpdateTime:              platform.UpdateTime,
		CreateTime:              platform.CreateTime,
		AsMessageChannel:        platform.AsMessageChannel,
		SendStreamIp:            platform.SendStreamIP,
		AutoPushChannel:         platform.AutoPushChannel,
		CatalogWithPlatform:     int32(platform.CatalogWithPlatform),
		CatalogWithGroup:        int32(platform.CatalogWithGroup),
		CatalogWithRegion:       int32(platform.CatalogWithRegion),
		CivilCode:               platform.CivilCode,
		Manufacturer:            platform.Manufacturer,
		Model:                   platform.Model,
		Address:                 platform.Address,
		RegisterWay:             int32(platform.RegisterWay),
		Secrecy:                 int32(platform.Secrecy),
	}

	resp.Code = 0
	resp.Message = "success"
	return resp, nil
}

// UpdatePlatform 实现更新平台信息
func (gb *GB28181Plugin) UpdatePlatform(ctx context.Context, req *pb.Platform) (resp *pb.BaseResponse, err error) {
	resp = &pb.BaseResponse{}

	if gb.DB == nil {
		resp.Code = 500
		resp.Message = "database not initialized"
		return resp, nil
	}

	// 检查平台是否存在
	var platform gb28181.PlatformModel
	if err := gb.DB.First(&platform, req.ServerGBId).Error; err != nil {
		resp.Code = 404
		resp.Message = "platform not found"
		return resp, nil
	}

	// 从请求中创建一个新的平台模型
	updatedPlatform := gb28181.PlatformModel{
		Enable:                  req.Enable,
		Name:                    req.Name,
		ServerGBID:              req.ServerGBId,
		ServerGBDomain:          req.ServerGBDomain,
		ServerIP:                req.ServerIp,
		ServerPort:              int(req.ServerPort),
		DeviceGBID:              req.DeviceGBId,
		DeviceIP:                req.DeviceIp,
		DevicePort:              int(req.DevicePort),
		Username:                req.Username,
		Password:                req.Password,
		Expires:                 int(req.Expires),
		KeepTimeout:             int(req.KeepTimeout),
		Transport:               req.Transport,
		CharacterSet:            req.CharacterSet,
		PTZ:                     req.Ptz,
		RTCP:                    req.Rtcp,
		Status:                  req.Status,
		ChannelCount:            int(req.ChannelCount),
		CatalogSubscribe:        req.CatalogSubscribe,
		AlarmSubscribe:          req.AlarmSubscribe,
		MobilePositionSubscribe: req.MobilePositionSubscribe,
		CatalogGroup:            int(req.CatalogGroup),
		UpdateTime:              req.UpdateTime,
		AsMessageChannel:        req.AsMessageChannel,
		SendStreamIP:            req.SendStreamIp,
		AutoPushChannel:         req.AutoPushChannel,
		CatalogWithPlatform:     int(req.CatalogWithPlatform),
		CatalogWithGroup:        int(req.CatalogWithGroup),
		CatalogWithRegion:       int(req.CatalogWithRegion),
		CivilCode:               req.CivilCode,
		Manufacturer:            req.Manufacturer,
		Model:                   req.Model,
		Address:                 req.Address,
		RegisterWay:             int(req.RegisterWay),
		Secrecy:                 int(req.Secrecy),
	}

	// 使用 GORM 的 Updates 方法更新非零值字段
	if err := gb.DB.Model(&platform).Updates(updatedPlatform).Error; err != nil {
		resp.Code = 500
		resp.Message = fmt.Sprintf("failed to update platform: %v", err)
		return resp, nil
	}
	gb.DB.Model(&platform).Find(&platform)
	// 处理平台启用状态变化
	if platform.Enable {
		// 如果存在旧的platform实例，先停止并移除
		if oldPlatform, ok := gb.platforms.Get(platform.ServerGBID); ok {
			oldPlatform.Unregister()
			oldPlatform.Stop(fmt.Errorf("platform updated"))
			gb.platforms.Remove(oldPlatform)
		}
		// 创建新的Platform实例
		platformInstance := NewPlatform(&platform, gb, false)
		// 添加到任务系统
		gb.AddTask(platformInstance)
	} else {
		// 如果平台被禁用，停止并移除旧的platform实例
		if oldPlatform, ok := gb.platforms.Get(platform.ServerGBID); ok {
			oldPlatform.Unregister()
			oldPlatform.Stop(fmt.Errorf("platform disabled"))
			gb.platforms.Remove(oldPlatform)
		}
	}

	resp.Code = 0
	resp.Message = "success"
	return resp, nil
}

// DeletePlatform 实现删除平台信息
func (gb *GB28181Plugin) DeletePlatform(ctx context.Context, req *pb.DeletePlatformRequest) (resp *pb.BaseResponse, err error) {
	resp = &pb.BaseResponse{}

	if gb.DB == nil {
		resp.Code = 500
		resp.Message = "database not initialized"
		return resp, nil
	}

	// 删除平台
	if err := gb.DB.Delete(&gb28181.PlatformModel{}, req.ServerGBID).Error; err != nil {
		resp.Code = 500
		resp.Message = fmt.Sprintf("failed to delete platform: %v", err)
		return resp, nil
	}

	resp.Code = 0
	resp.Message = "success"
	return resp, nil
}

// ListPlatforms 实现获取平台列表
func (gb *GB28181Plugin) ListPlatforms(ctx context.Context, req *pb.ListPlatformsRequest) (resp *pb.PlatformsPageInfo, err error) {
	resp = &pb.PlatformsPageInfo{}

	if gb.DB == nil {
		resp.Code = 500
		resp.Message = "database not initialized"
		return resp, nil
	}

	var platforms []gb28181.PlatformModel
	var total int64

	// 构建查询条件
	query := gb.DB.Model(&gb28181.PlatformModel{})
	if req.Query != "" {
		query = query.Where("name LIKE ? OR server_gb_id LIKE ? OR device_gb_id LIKE ?",
			"%"+req.Query+"%", "%"+req.Query+"%", "%"+req.Query+"%")
	}
	if req.Status {
		query = query.Where("status = ?", true)
	}

	// 获取总数
	if err := query.Count(&total).Error; err != nil {
		resp.Code = 500
		resp.Message = fmt.Sprintf("failed to count platforms: %v", err)
		return resp, nil
	}

	// 查询平台列表
	// 当Page和Count都为0时，不做分页，返回所有数据
	if req.Page == 0 && req.Count == 0 {
		// 不分页，查询所有数据
		if err := query.Find(&platforms).Error; err != nil {
			resp.Code = 500
			resp.Message = fmt.Sprintf("failed to list platforms: %v", err)
			return resp, nil
		}
	} else {
		// 分页查询
		if err := query.Offset(int(req.Page-1) * int(req.Count)).
			Limit(int(req.Count)).
			Find(&platforms).Error; err != nil {
			resp.Code = 500
			resp.Message = fmt.Sprintf("failed to list platforms: %v", err)
			return resp, nil
		}
	}

	// 转换为proto消息
	var pbPlatforms []*pb.Platform
	for _, p := range platforms {
		pbPlatforms = append(pbPlatforms, &pb.Platform{
			Enable:                  p.Enable,
			Name:                    p.Name,
			ServerGBId:              p.ServerGBID,
			ServerGBDomain:          p.ServerGBDomain,
			ServerIp:                p.ServerIP,
			ServerPort:              int32(p.ServerPort),
			DeviceGBId:              p.DeviceGBID,
			DeviceIp:                p.DeviceIP,
			DevicePort:              int32(p.DevicePort),
			Username:                p.Username,
			Password:                p.Password,
			Expires:                 int32(p.Expires),
			KeepTimeout:             int32(p.KeepTimeout),
			Transport:               p.Transport,
			CharacterSet:            p.CharacterSet,
			Ptz:                     p.PTZ,
			Rtcp:                    p.RTCP,
			Status:                  p.Status,
			ChannelCount:            int32(p.ChannelCount),
			CatalogSubscribe:        p.CatalogSubscribe,
			AlarmSubscribe:          p.AlarmSubscribe,
			MobilePositionSubscribe: p.MobilePositionSubscribe,
			CatalogGroup:            int32(p.CatalogGroup),
			UpdateTime:              p.UpdateTime,
			CreateTime:              p.CreateTime,
			AsMessageChannel:        p.AsMessageChannel,
			SendStreamIp:            p.SendStreamIP,
			AutoPushChannel:         p.AutoPushChannel,
			CatalogWithPlatform:     int32(p.CatalogWithPlatform),
			CatalogWithGroup:        int32(p.CatalogWithGroup),
			CatalogWithRegion:       int32(p.CatalogWithRegion),
			CivilCode:               p.CivilCode,
			Manufacturer:            p.Manufacturer,
			Model:                   p.Model,
			Address:                 p.Address,
			RegisterWay:             int32(p.RegisterWay),
			Secrecy:                 int32(p.Secrecy),
		})
	}

	resp.Total = int32(total)
	resp.List = pbPlatforms
	resp.Code = 0
	resp.Message = "success"
	return resp, nil
}

// QueryRecord 实现录像查询接口
func (gb *GB28181Plugin) QueryRecord(ctx context.Context, req *pb.QueryRecordRequest) (resp *pb.QueryRecordResponse, err error) {
	resp = &pb.QueryRecordResponse{
		Code:    0,
		Message: "",
	}
	startTime, endTime, err := util.TimeRangeQueryParse(url.Values{"range": []string{req.Range}, "start": []string{req.Start}, "end": []string{req.End}})
	// 获取设备和通道
	device, ok := gb.devices.Get(req.DeviceId)
	if !ok {
		resp.Code = 404
		resp.Message = "device not found"
		return resp, nil
	}

	channel, ok := device.channels.Get(req.DeviceId + "_" + req.ChannelId)
	if !ok {
		resp.Code = 404
		resp.Message = "channel not found"
		return resp, nil
	}

	// 生成随机序列号
	sn := int(time.Now().UnixNano() / 1e6 % 1000000)

	// 发送录像查询请求
	promise, err := gb.RecordInfoQuery(req.DeviceId, req.ChannelId, startTime, endTime, sn)
	if err != nil {
		resp.Code = 500
		resp.Message = err.Error()
		return resp, nil
	}

	// 等待响应
	err = promise.Await()
	if err != nil {
		resp.Code = 500
		resp.Message = fmt.Sprintf("query failed: %v", err)
		return resp, nil
	}

	// 获取录像请求
	recordReq, ok := channel.RecordReqs.Get(sn)
	if !ok {
		resp.Code = 500
		resp.Message = "record request not found"
		return resp, nil
	}

	// 转换结果
	if len(recordReq.Response) > 0 {
		firstResponse := recordReq.Response[0]
		resp.DeviceId = req.DeviceId
		resp.ChannelId = req.ChannelId
		resp.Name = firstResponse.Name
		resp.Count = int32(recordReq.ReceivedNum)
		if !firstResponse.LastTime.IsZero() {
			resp.LastTime = timestamppb.New(firstResponse.LastTime)
		}
	}

	for _, record := range recordReq.Response {
		for _, item := range record.RecordList.Item {
			resp.Data = append(resp.Data, &pb.RecordItem{
				DeviceId:   item.DeviceID,
				Name:       item.Name,
				FilePath:   item.FilePath,
				Address:    item.Address,
				StartTime:  item.StartTime,
				EndTime:    item.EndTime,
				Secrecy:    int32(item.Secrecy),
				Type:       item.Type,
				RecorderId: item.RecorderID,
			})
		}
	}

	resp.Code = 0
	resp.Message = fmt.Sprintf("success, received %d/%d records", recordReq.ReceivedNum, recordReq.SumNum)

	// 排序录像列表，按StartTime升序排序
	sort.Slice(resp.Data, func(i, j int) bool {
		return resp.Data[i].StartTime < resp.Data[j].StartTime
	})

	// 清理请求
	channel.RecordReqs.Remove(recordReq)

	return resp, nil
}

// PtzControl 实现云台控制功能
func (gb *GB28181Plugin) PtzControl(ctx context.Context, req *pb.PtzControlRequest) (resp *pb.BaseResponse, err error) {
	resp = &pb.BaseResponse{}

	// 参数校验
	if req.DeviceId == "" {
		resp.Code = 400
		resp.Message = "设备ID不能为空"
		return resp, nil
	}
	if req.ChannelId == "" {
		resp.Code = 400
		resp.Message = "通道ID不能为空"
		return resp, nil
	}

	// 获取设备
	device, ok := gb.devices.Get(req.DeviceId)
	if !ok {
		resp.Code = 404
		resp.Message = "设备不存在"
		return resp, nil
	}

	// 调用设备的前端控制命令
	response, err := device.frontEndCmd(req.ChannelId, req.Ptzcmd)
	if err != nil {
		resp.Code = 500
		resp.Message = fmt.Sprintf("发送云台控制命令失败: %v", err)
		return resp, nil
	}

	gb.Info("云台控制",
		"deviceId", req.DeviceId,
		"channelId", req.ChannelId,
		"Ptzcmd", req.Ptzcmd,
		"response", response.String())

	resp.Code = 0
	resp.Message = "success"
	return resp, nil
}

// IrisControl implements the GB28181Plugin interface.
func (s *GB28181Plugin) IrisControl(ctx context.Context, req *pb.IrisControlRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// FocusControl implements the GB28181Plugin interface.
func (s *GB28181Plugin) FocusControl(ctx context.Context, req *pb.FocusControlRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// QueryPreset implements the GB28181Plugin interface.
func (s *GB28181Plugin) QueryPreset(ctx context.Context, req *pb.PresetRequest) (resp *pb.PresetResponse, err error) {
	// TODO: Your code here...
	return
}

// AddPreset implements the GB28181Plugin interface.
func (s *GB28181Plugin) AddPreset(ctx context.Context, req *pb.PresetRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// CallPreset implements the GB28181Plugin interface.
func (s *GB28181Plugin) CallPreset(ctx context.Context, req *pb.PresetRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// DeletePreset implements the GB28181Plugin interface.
func (s *GB28181Plugin) DeletePreset(ctx context.Context, req *pb.PresetRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// AddCruisePoint implements the GB28181Plugin interface.
func (s *GB28181Plugin) AddCruisePoint(ctx context.Context, req *pb.CruisePointRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// DeleteCruisePoint implements the GB28181Plugin interface.
func (s *GB28181Plugin) DeleteCruisePoint(ctx context.Context, req *pb.CruisePointRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// SetCruiseSpeed implements the GB28181Plugin interface.
func (s *GB28181Plugin) SetCruiseSpeed(ctx context.Context, req *pb.CruiseSpeedRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// SetCruiseTime implements the GB28181Plugin interface.
func (s *GB28181Plugin) SetCruiseTime(ctx context.Context, req *pb.CruiseTimeRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// StartCruise implements the GB28181Plugin interface.
func (s *GB28181Plugin) StartCruise(ctx context.Context, req *pb.CruiseRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// StopCruise implements the GB28181Plugin interface.
func (s *GB28181Plugin) StopCruise(ctx context.Context, req *pb.CruiseRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// StartScan implements the GB28181Plugin interface.
func (s *GB28181Plugin) StartScan(ctx context.Context, req *pb.ScanRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// StopScan implements the GB28181Plugin interface.
func (s *GB28181Plugin) StopScan(ctx context.Context, req *pb.ScanRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// SetScanLeft implements the GB28181Plugin interface.
func (s *GB28181Plugin) SetScanLeft(ctx context.Context, req *pb.ScanRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// SetScanRight implements the GB28181Plugin interface.
func (s *GB28181Plugin) SetScanRight(ctx context.Context, req *pb.ScanRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// SetScanSpeed implements the GB28181Plugin interface.
func (s *GB28181Plugin) SetScanSpeed(ctx context.Context, req *pb.ScanSpeedRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// WiperControl implements the GB28181Plugin interface.
func (s *GB28181Plugin) WiperControl(ctx context.Context, req *pb.WiperControlRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// AuxiliaryControl implements the GB28181Plugin interface.
func (s *GB28181Plugin) AuxiliaryControl(ctx context.Context, req *pb.AuxiliaryControlRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// TestSip implements the GB28181Plugin interface.
func (s *GB28181Plugin) TestSip(ctx context.Context, req *pb.TestSipRequest) (resp *pb.TestSipResponse, err error) {
	// TODO: Your code here...
	return
}

// SearchAlarms implements the GB28181Plugin interface.
func (s *GB28181Plugin) SearchAlarms(ctx context.Context, req *pb.SearchAlarmsRequest) (resp *pb.SearchAlarmsResponse, err error) {
	// TODO: Your code here...
	return
}

// AddPlatformChannel implements the GB28181Plugin interface.
func (s *GB28181Plugin) AddPlatformChannel(ctx context.Context, req *pb.AddPlatformChannelRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// Recording implements the GB28181Plugin interface.
func (s *GB28181Plugin) Recording(ctx context.Context, req *pb.RecordingRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// UploadJpeg implements the GB28181Plugin interface.
func (s *GB28181Plugin) UploadJpeg(ctx context.Context, req *pb.UploadJpegRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// PlaybackPause implements the GB28181Plugin interface.
func (s *GB28181Plugin) PlaybackPause(ctx context.Context, req *pb.PlaybackPauseRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// PlaybackResume implements the GB28181Plugin interface.
func (s *GB28181Plugin) PlaybackResume(ctx context.Context, req *pb.PlaybackResumeRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// PlaybackSeek implements the GB28181Plugin interface.
func (s *GB28181Plugin) PlaybackSeek(ctx context.Context, req *pb.PlaybackSeekRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// PlaybackSpeed implements the GB28181Plugin interface.
func (s *GB28181Plugin) PlaybackSpeed(ctx context.Context, req *pb.PlaybackSpeedRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// GetGroups implements the GB28181Plugin interface.
func (s *GB28181Plugin) GetGroups(ctx context.Context, req *pb.GetGroupsRequest) (resp *pb.GroupsListResponse, err error) {
	// TODO: Your code here...
	return
}

// AddGroup implements the GB28181Plugin interface.
func (s *GB28181Plugin) AddGroup(ctx context.Context, req *pb.Group) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// UpdateGroup implements the GB28181Plugin interface.
func (s *GB28181Plugin) UpdateGroup(ctx context.Context, req *pb.Group) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// DeleteGroup implements the GB28181Plugin interface.
func (s *GB28181Plugin) DeleteGroup(ctx context.Context, req *pb.DeleteGroupRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// AddGroupChannel implements the GB28181Plugin interface.
func (s *GB28181Plugin) AddGroupChannel(ctx context.Context, req *pb.AddGroupChannelRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// DeleteGroupChannel implements the GB28181Plugin interface.
func (s *GB28181Plugin) DeleteGroupChannel(ctx context.Context, req *pb.DeleteGroupChannelRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// GetGroupChannels implements the GB28181Plugin interface.
func (s *GB28181Plugin) GetGroupChannels(ctx context.Context, req *pb.GetGroupChannelsRequest) (resp *pb.GroupChannelsResponse, err error) {
	// TODO: Your code here...
	return
}

// RemoveDevice implements the GB28181Plugin interface.
func (s *GB28181Plugin) RemoveDevice(ctx context.Context, req *pb.RemoveDeviceRequest) (resp *pb.BaseResponse, err error) {
	// TODO: Your code here...
	return
}

// OpenRTPServer implements the GB28181Plugin interface.
func (s *GB28181Plugin) OpenRTPServer(ctx context.Context, req *pb.OpenRTPServerRequest) (resp *pb.OpenRTPServerResponse, err error) {
	// TODO: Your code here...
	return
}
