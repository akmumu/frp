// Copyright 2016 fatedier, fatedier@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"encoding/json"

	"frp/models/consts"
	"frp/models/msg"
	"frp/utils/conn"
	"frp/utils/log"
)

type ProxyClient struct {
	Name      string
	Passwd    string
	LocalIp   string
	LocalPort int64
}

func (p *ProxyClient) GetLocalConn() (c *conn.Conn, err error) {
	c, err = conn.ConnectServer(p.LocalIp, p.LocalPort)
	if err != nil {
		log.Error("ProxyName [%s], connect to local port error, %v", p.Name, err)
	}
	return
}

func (p *ProxyClient) GetRemoteConn(addr string, port int64) (c *conn.Conn, err error) {
	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	c, err = conn.ConnectServer(addr, port)
	if err != nil {
		log.Error("ProxyName [%s], connect to server [%s:%d] error, %v", p.Name, addr, port, err)
		return
	}

	req := &msg.ClientCtlReq{
		Type:      consts.WorkConn,
		ProxyName: p.Name,
		Passwd:    p.Passwd,
	}

	buf, _ := json.Marshal(req)
	err = c.Write(string(buf) + "\n")
	if err != nil {
		log.Error("ProxyName [%s], write to server error, %v", p.Name, err)
		return
	}

	err = nil
	return
}

func (p *ProxyClient) StartTunnel(serverAddr string, serverPort int64) (err error) {
	// TODO 根据配置中的一个本地端口，链接一下，比如是22或者80啥的
	localConn, err := p.GetLocalConn()
	if err != nil {
		return
	}
	// TODO 根据远程端口，也链接上
	remoteConn, err := p.GetRemoteConn(serverAddr, serverPort)
	if err != nil {
		return
	}

	// l means local, r means remote
	log.Debug("Join two conns, (l[%s] r[%s]) (l[%s] r[%s])", localConn.GetLocalAddr(), localConn.GetRemoteAddr(),
		remoteConn.GetLocalAddr(), remoteConn.GetRemoteAddr())
	// TODO 核心中的核心，两个连接都链接上了，让他俩在一起，开始互相数据
	// TODO 思考一下，其实啊，这边的client才像是个server，而我们的server像是个二级代理分销商
	go conn.Join(localConn, remoteConn)
	return nil
}
