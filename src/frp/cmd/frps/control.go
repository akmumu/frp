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

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"frp/models/consts"
	"frp/models/msg"
	"frp/models/server"
	"frp/utils/conn"
	"frp/utils/log"
)

// TODO: 处理刚才server启动好的一个server中的数据
func ProcessControlConn(l *conn.Listener) {
	for {
		// TODO: 不断获取的连接
		c, err := l.GetConn()
		if err != nil {
			return
		}
		log.Debug("Get one new conn, %v", c.GetRemoteAddr())
		// TODO: 启动一个goroutine来处理这个连接
		go controlWorker(c)
	}
}

// connection from every client and server
func controlWorker(c *conn.Conn) {
	// the first message is from client to server
	// if error, close connection
	res, err := c.ReadLine()
	if err != nil {
		log.Warn("Read error, %v", err)
		return
	}
	log.Debug("get: %s", res)

	/**
	TODO 可以看到这是客户端发过来的消息，主要有一个proxyname，对应配置文件中那一对
	type ClientCtlReq struct {
		Type      int64  `json:"type"`
		ProxyName string `json:"proxy_name"`
		Passwd    string `json:"passwd"`
	}
	*/
	clientCtlReq := &msg.ClientCtlReq{}
	clientCtlRes := &msg.ClientCtlRes{}
	if err := json.Unmarshal([]byte(res), &clientCtlReq); err != nil {
		log.Warn("Parse err: %v : %s", err, res)
		return
	}

	// check
	succ, info, needRes := checkProxy(clientCtlReq, c)
	if !succ {
		clientCtlRes.Code = 1
		clientCtlRes.Msg = info
	}

	if needRes {
		defer c.Close()

		buf, _ := json.Marshal(clientCtlRes)
		err = c.Write(string(buf) + "\n")
		if err != nil {
			log.Warn("Write error, %v", err)
			time.Sleep(1 * time.Second)
			return
		}
	} else {
		// work conn, just return
		return
	}

	// other messages is from server to client
	// TODO 这个proxy server怎么来的呢，在配置加载的时候
	s, ok := server.ProxyServers[clientCtlReq.ProxyName]
	if !ok {
		log.Warn("ProxyName [%s] is not exist", clientCtlReq.ProxyName)
		return
	}

	// read control msg from client
	// TODO:这块是处理一些内部控制相关的消息，比如c到s的心跳检测
	go readControlMsgFromClient(s, c)

	serverCtlReq := &msg.ClientCtlReq{}
	serverCtlReq.Type = consts.WorkConn
	for {
		closeFlag := s.WaitUserConn()
		if closeFlag {
			log.Debug("ProxyName [%s], goroutine for dealing user conn is closed", s.Name)
			break
		}
		buf, _ := json.Marshal(serverCtlReq)
		err = c.Write(string(buf) + "\n")
		if err != nil {
			log.Warn("ProxyName [%s], write to client error, proxy exit", s.Name)
			s.Close()
			return
		}

		log.Debug("ProxyName [%s], write to client to add work conn success", s.Name)
	}

	log.Info("ProxyName [%s], I'm dead!", s.Name)
	return
}

func checkProxy(req *msg.ClientCtlReq, c *conn.Conn) (succ bool, info string, needRes bool) {
	succ = false
	needRes = true
	// check if proxy name exist
	s, ok := server.ProxyServers[req.ProxyName]
	if !ok {
		info = fmt.Sprintf("ProxyName [%s] is not exist", req.ProxyName)
		log.Warn(info)
		return
	}

	// check password
	if req.Passwd != s.Passwd {
		info = fmt.Sprintf("ProxyName [%s], password is not correct", req.ProxyName)
		log.Warn(info)
		return
	}

	// control conn
	if req.Type == consts.CtlConn {
		if s.Status != consts.Idle {
			info = fmt.Sprintf("ProxyName [%s], already in use", req.ProxyName)
			log.Warn(info)
			return
		}

		// start proxy and listen for user conn, no block
		// TODO 启动proxy，并监听用户连接，不阻塞，其中client给他发送的一些代理们，这里会进行处理
		err := s.Start()
		if err != nil {
			info = fmt.Sprintf("ProxyName [%s], start proxy error: %v", req.ProxyName, err.Error())
			log.Warn(info)
			return
		}

		log.Info("ProxyName [%s], start proxy success", req.ProxyName)
	} else if req.Type == consts.WorkConn {
		// work conn
		needRes = false
		if s.Status != consts.Working {
			log.Warn("ProxyName [%s], is not working when it gets one new work conn", req.ProxyName)
			return
		}
		// TODO 干活的链接，我们把他也扔到队列里，然后proxy server去处理
		s.GetNewCliConn(c)
	} else {
		info = fmt.Sprintf("ProxyName [%s], type [%d] unsupport", req.ProxyName, req.Type)
		log.Warn(info)
		return
	}

	succ = true
	return
}

func readControlMsgFromClient(s *server.ProxyServer, c *conn.Conn) {
	isContinueRead := true
	f := func() {
		isContinueRead = false
		s.Close()
		log.Error("ProxyName [%s], client heartbeat timeout", s.Name)
	}
	timer := time.AfterFunc(time.Duration(server.HeartBeatTimeout)*time.Second, f)
	defer timer.Stop()

	for isContinueRead {
		content, err := c.ReadLine()
		if err != nil {
			if err == io.EOF {
				log.Warn("ProxyName [%s], client is dead!", s.Name)
				s.Close()
				break
			} else if nil == c || c.IsClosed() {
				log.Warn("ProxyName [%s], client connection is closed", s.Name)
				break
			}

			log.Error("ProxyName [%s], read error: %v", s.Name, err)
			continue
		}

		clientCtlReq := &msg.ClientCtlReq{}
		if err := json.Unmarshal([]byte(content), clientCtlReq); err != nil {
			log.Warn("Parse err: %v : %s", err, content)
			continue
		}
		if consts.CSHeartBeatReq == clientCtlReq.Type {
			log.Debug("ProxyName [%s], get heartbeat", s.Name)
			timer.Reset(time.Duration(server.HeartBeatTimeout) * time.Second)

			clientCtlRes := &msg.ClientCtlRes{}
			clientCtlRes.GeneralRes.Code = consts.SCHeartBeatRes
			response, err := json.Marshal(clientCtlRes)
			if err != nil {
				log.Warn("Serialize ClientCtlRes err! err: %v", err)
				continue
			}

			err = c.Write(string(response) + "\n")
			if err != nil {
				log.Error("Send heartbeat response to client failed! Err:%v", err)
				continue
			}
		}
	}
}
